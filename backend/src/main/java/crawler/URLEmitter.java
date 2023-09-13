package crawler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * Emits discovered urls to global SQS queues (multiplexed based on hash of host name)
 * @author Daniel
 *
 */
public class URLEmitter {
    final static Logger logger = LogManager.getLogger(URLEmitter.class);
    final int MAX_BATCH_SIZE = 10;
    AmazonSQS sqs;
    CrawlStateTracker tracker;
    int updateFrequency;
    int counter;
    int workerIndex;
    int numWorkers;
    List<String> workerQueueURLs;
    Storage storage;
    
    public URLEmitter(AmazonSQS sqs, List<String> workerQueueURLs, CrawlStateTracker tracker, 
            Storage storage, int updateFrequency, int workerIndex, int numWorkers) {
        this.sqs = sqs;
        this.workerQueueURLs = workerQueueURLs;
        this.tracker = tracker;
        this.updateFrequency = updateFrequency;
        this.workerIndex = workerIndex;
        this.numWorkers = numWorkers;
        this.storage = storage;
        counter = 0;
    }
    
    /**
     * Hashes the host name and returns a hash value between 0 and the numWorkers-1
     * @param url
     * @return
     */
    public int getTargetWorkerIndex(String url) {
        URLInfo urlInfo = new URLInfo(url);
        String hostName = urlInfo.getHostName();
        if (hostName == null) {
            tracker.incNumUnhashableHosts();
            return -1;
        }
        return Math.abs(hostName.hashCode()) % numWorkers;
    }
    
    /**
     * Emits a single url to the appropriate SQS queue, multiplexed by host name
     * @param url
     */
    public synchronized void emitURL(String url) {
        if (url == null || url.equals("")) {
            return;
        }
        JSONObject messageObj = new JSONObject();
        messageObj.put("MSG_TYPE", "URL");
        messageObj.put("URL", url);
        
        int targetWorkerInd = getTargetWorkerIndex(url);
        if (targetWorkerInd < 0) {
            logger.debug("Could not hash: " + url);
            return;
        }
        String targetWorkerQueueURL = workerQueueURLs.get(targetWorkerInd);
        
        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(targetWorkerQueueURL)
                .withMessageBody(messageObj.toString());
        sqs.sendMessage(send_msg_request);
        
        
    }
    
    /**
     * Emits a list of URLs to the appropriate SQS queues, multiplexed by host name
     * @param emissions
     */
    public synchronized void emitURLs(List<String> emissions) {
        Map<Integer, List<SendMessageBatchRequestEntry>> outgoingMessages = new HashMap<>();
        JSONObject messageObj;
        int targetWorkerIndex;
        SendMessageBatchRequestEntry entry;
        for (String emission : emissions) {
            if (emission == null || emission.equals("")) {
                continue;
            }
            
            messageObj = new JSONObject();
            messageObj.put("MSG_TYPE", "URL");
            messageObj.put("URL", emission);
            entry = new SendMessageBatchRequestEntry(
                    String.valueOf(counter), messageObj.toString());
            targetWorkerIndex = getTargetWorkerIndex(emission);
            if (targetWorkerIndex < 0) {
                logger.error("Could not hash: " + emission);
                continue;
            }
            if (!outgoingMessages.containsKey(targetWorkerIndex)) {
                outgoingMessages.put(targetWorkerIndex, new ArrayList<>());
            }
            outgoingMessages.get(targetWorkerIndex).add(entry);
            logger.debug("Emitting toward worker " + targetWorkerIndex + " url: " + emission);
            
            counter++;
            if (counter % updateFrequency == 0) {
                sendUpdate();
                storage.flush();
            }
        }
        
        SendMessageBatchRequest send_batch_request;
        String targetWorkerQueueURL;
        int endIndex;
        for (Integer i : outgoingMessages.keySet()) {
            targetWorkerQueueURL = workerQueueURLs.get(i);
            for (int startIndex = 0; startIndex < outgoingMessages.get(i).size(); startIndex += MAX_BATCH_SIZE) {
                endIndex = Math.min(startIndex + MAX_BATCH_SIZE, outgoingMessages.get(i).size());
                send_batch_request = new SendMessageBatchRequest()
                        .withQueueUrl(targetWorkerQueueURL)
                        .withEntries(outgoingMessages.get(i).subList(startIndex, endIndex));
                sqs.sendMessageBatch(send_batch_request);
            }
            
        }
    }
    
    /**
     * Sends a crawl update message (containing the number of documents crawled by the current instance)
     * to all other SQS worker queues
     */
    public synchronized void sendUpdate() {
        JSONObject messageObj = new JSONObject();
        messageObj.put("MSG_TYPE", "CRAWL_UDPATE");
        messageObj.put("WORKER_INDEX", workerIndex);
        messageObj.put("NUM_CRAWLED", tracker.getLocalNumIndexed());
        SendMessageRequest send_msg_request = null;
        
        String targetWorkerQueueURL;
        for (int i = 0; i < numWorkers; i++) {
            if (i == workerIndex) {continue;}
            targetWorkerQueueURL = workerQueueURLs.get(i);
            send_msg_request = new SendMessageRequest()
                    .withQueueUrl(targetWorkerQueueURL)
                    .withMessageBody(messageObj.toString());
            sqs.sendMessage(send_msg_request);
        }
        if (send_msg_request != null) {
            logger.info("Sending update: " + send_msg_request.toString());
        }
        logger.info(tracker.getLocalTrackState());
        
        if (tracker.isTargetReached()) {
            endCrawl();
            logger.info("Target reached (emitter)");
        }
        
    }
    
    /**
     * Emits and end-crawl message to all other workers, and updates the local state tracker
     * to set the end-crawl flag.
     */
    public synchronized void endCrawl() {
        tracker.endCrawl();
        JSONObject messageObj = new JSONObject();
        messageObj.put("MSG_TYPE", "END_CRAWL");
        SendMessageRequest send_msg_request = null;
        for (String targetWorkerQueueURL : workerQueueURLs) {
            send_msg_request = new SendMessageRequest()
                    .withQueueUrl(targetWorkerQueueURL)
                    .withMessageBody(messageObj.toString());
            sqs.sendMessage(send_msg_request);
        }
    }

}
