package crawler;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import com.amazonaws.services.sqs.model.Message;

/**
 * Handles parsing incoming messages from SQS queue
 * @author Daniel
 *
 */
public class MessageHandler {
    final static Logger logger = LogManager.getLogger(MessageHandler.class);
    CrawlStateTracker tracker;
    Storage storage;
    URLEmitter emitter;
    ConcurrentLinkedQueue<String> urlQueue;
    
    public MessageHandler(ConcurrentLinkedQueue<String> urlQueue, CrawlStateTracker tracker, Storage storage, URLEmitter emitter) {
        this.tracker = tracker;
        this.storage = storage;
        this.emitter = emitter;
        this.urlQueue = urlQueue;
    }
    
    /**
     * Processes incoming SQS message. If message contains a url, adds it 
     * to local queue for processing. If message is an update from another worker,
     * updates local state tracking. If message signals end of crawl, terminates the current
     * crawler instance.
     * @param m
     */
    public void handleMessage(Message m) {
        JSONObject jsonMessage = new JSONObject(m.getBody());
        switch (jsonMessage.getString("MSG_TYPE")) {
        case "URL": // new url to be processed
            tracker.incNumMessagesReceived();
            String url = jsonMessage.getString("URL");
            logger.debug("Received url: " + url);
            urlQueue.add(url);
            return;
        case "CRAWL_UDPATE": // update from another crawler instance
            int workerIndex = jsonMessage.getInt("WORKER_INDEX");
            int numCrawled = jsonMessage.getInt("NUM_CRAWLED");
            handleCrawlUpdate(workerIndex, numCrawled);
            logger.info("Received update from worker " + workerIndex + " numcrawled: " + numCrawled);
            return;
        case "END_CRAWL": // time to end crawl
            logger.info("Received END_CRAWL");
            tracker.endCrawl();
            return;
        default:
            return;
        }
    }
     
    /**
     * Handles crawl update from another crawler instance: updates local crawl
     * state tracking and checks whether it is time to end the crawl.
     * @param workerIndex
     * @param numCrawled
     */
    public void handleCrawlUpdate(int workerIndex, int numCrawled) {
        tracker.updateNumIndexed(workerIndex, numCrawled);
        if (!tracker.isTargetReached()) {
            return;
        }
        logger.info("Target Reached");
        emitter.endCrawl();
        return;
    }
    
    
}
