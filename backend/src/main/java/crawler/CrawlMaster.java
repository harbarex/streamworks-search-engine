package crawler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;

/**
 * Coordinator for crawler workers
 * @author Daniel
 *
 */
public class CrawlMaster {
    final static Logger logger = LogManager.getLogger(CrawlMaster.class);
    static String queueName;
    static int workerIndex;
    static int numWorkers;
    static int targetNumCrawled;
    static String seedFilePath;
    static String storagePath;
    static int maxDocSizeBytes;
    static int updateFrequency;
    static int threadPoolSize;
    static int maxMessageBufferSize;
    static int maxURLEmits;
    static int readTimeoutMillis;
    static int connectTimeoutMillis;
    static int filterKeywordThreshold;
    static double minProbability;
    static int minBadDomainFails;
    static double minBadDomainFailRatio;
    static String crawlerName;
    static String keywordPath;
    
    
    /**
     * Parses the command line arguments and config file
     * @param args
     */
    public static void parseArgs(String[] args) {
        if (args.length < 6) {
            System.out.println("Args: (worker index) (num workers) (num docs) (config path) (seed url path) (keywords path)");
            System.exit(1);
        }
        workerIndex = Integer.valueOf(args[0]);
        numWorkers = Integer.valueOf(args[1]);
        targetNumCrawled = Integer.valueOf(args[2]);
        String configPath = args[3];
        seedFilePath = args[4];
        keywordPath = args[5];
        JSONObject config = FileUtils.readConfig(configPath);
        storagePath = config.has("storagePath") ? config.getString("storagePath") : "storage";
        maxDocSizeBytes = config.has("maxDocSizeBytes") ? config.getInt("maxDocSizeBytes") : 390 * ((int) Math.pow(2, 10));
        updateFrequency = config.has("updateFrequency") ? config.getInt("updateFrequency") : 500;
        threadPoolSize = config.has("threadPoolSize") ? config.getInt("threadPoolSize") : 1;
        maxURLEmits = config.has("maxURLEmits") ? config.getInt("maxURLEmits") : 400;
        maxMessageBufferSize = config.has("maxMessageBufferSize") ? config.getInt("maxMessageBufferSize") : 100;
        readTimeoutMillis = config.has("readTimeoutMillis") ? config.getInt("readTimeoutMillis") : 1000;
        connectTimeoutMillis = config.has("connectTimeoutMillis") ? config.getInt("connectTimeoutMillis") : 500;
        filterKeywordThreshold = config.has("filterKeywordThreshold") ? config.getInt("filterKeywordThreshold") : 4;
        minProbability = config.has("minProbability") ? config.getDouble("minProbability") : 0.01;
        minBadDomainFails = config.has("minBadDomainFails") ? config.getInt("minBadDomainFails") : 50;
        minBadDomainFailRatio = config.has("minBadDomainFailRatio") ? config.getDouble("minBadDomainFailRatio") : 0.75;
        crawlerName = config.has("crawlerName") ? config.getString("crawlerName") : "cis455crawler";
        queueName = config.has("queueName") ? config.getString("queueName") : "CrawlerURLQueue";
    }

    public static void main(String[] args) {
        org.apache.logging.log4j.core.config.Configurator.setLevel("crawler", Level.INFO);
        parseArgs(args);
        
        
        Storage storage = null;
        AmazonSQS sqs = null;
        try {
            storage = new Storage(storagePath);
            sqs = AmazonSQSClientBuilder.defaultClient();
            ConcurrentLinkedQueue<String> urlQueue = new ConcurrentLinkedQueue<>();
            CrawlStateTracker tracker = new CrawlStateTracker(workerIndex, targetNumCrawled, storage.getNumDocs());
            List<String> workerQueueURLs = getQueueURLs(sqs, numWorkers);
            String currentWorkerQueue = workerQueueURLs.get(workerIndex);
            URLEmitter emitter = new URLEmitter(sqs, workerQueueURLs, tracker, storage, updateFrequency, workerIndex, numWorkers);
            MessageHandler msgHandler = new MessageHandler(urlQueue, tracker, storage, emitter);
            emitSeedURLs(emitter);
            List<String> keywords = FileUtils.getFileLines(keywordPath);
            
            Crawler[] crawlers = new Crawler[threadPoolSize];
            Thread[] crawlerThreads = new Thread[threadPoolSize];
            for (int i = 0; i < threadPoolSize; i++) {
                crawlers[i] = new Crawler(storage, tracker, emitter, urlQueue, keywords, i);
                crawlerThreads[i] = new Thread(crawlers[i]);
            }
            
            for (int i = 0; i < threadPoolSize; i++) {
                crawlerThreads[i].start();
            }
            
            
            
            List<Message> messages;
            while (!tracker.isCrawlEnded()) {
                messages = sqs.receiveMessage(currentWorkerQueue).getMessages();
                if (messages.size() != 0) {
                    logger.debug("Received messages: " + messages.size());
                }
                for (Message m : messages) {
                    msgHandler.handleMessage(m);
                    sqs.deleteMessage(currentWorkerQueue, m.getReceiptHandle());
                }
                synchronized (urlQueue) {
                    urlQueue.notifyAll();
                }
                logger.debug("Waiting for queue to shrink");
                tracker.waitUntilWithinThreshold(urlQueue, maxMessageBufferSize);
                if (messages.size() != 0) {
                    logger.debug("Waiting for messages");
                }
            }
            logger.info("Emptying queue");
            urlQueue.clear();
            logger.info("Waiting for threads to end");
            tracker.waitUntilThreadsInactive();
            logger.info("Interrupting threads");
            for (int i = 0; i < threadPoolSize; i++) {
                crawlerThreads[i].interrupt();
            }
            
            logger.info("Num docs: " + storage.getNumDocs());
            logger.info("Num edges: " + storage.getNumEdges());
        }
        finally {
            if (storage != null) {
                storage.close();
            }
            if (sqs != null) {
                sqs.shutdown();
            }
        }
    }
    
    /**
     * Returns the url of the sqs worker queue with the specified index
     * @param sqs
     * @param workerIndex
     * @return
     */
    public static String getQueueURL(AmazonSQS sqs, int workerIndex) {
        return sqs.getQueueUrl(queueName + workerIndex).getQueueUrl();
    }
    
    /**
     * Returns a list of all the sqs worker queues
     * @param sqs
     * @param numWorkers
     * @return
     */
    public static List<String> getQueueURLs(AmazonSQS sqs, int numWorkers) {
        List<String> urls = new ArrayList<String>();
        for (int i = 0; i < numWorkers; i++) {
            urls.add(getQueueURL(sqs, i));
        }
        return urls;
    }

    /**
     * Parses the seed url file and emits each url to the global queue
     * @param emitter
     */
    private static void emitSeedURLs(URLEmitter emitter) {
        List<String> seedURLs = FileUtils.getFileLines(seedFilePath);
        if (seedURLs.size() == 0) {
            logger.info("No seed URLs");
        }
        List<String> emissions = new ArrayList<>();
        for (String url : seedURLs) {
            if (!url.startsWith("http")) {
                url = "https://" + url;
            }
            emissions.add(url);
        }
        emitter.emitURLs(emissions);
    }

    
    

}
