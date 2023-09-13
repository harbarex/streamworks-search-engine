package crawler;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * Class for tracking status of crawler run.
 * @author Daniel
 *
 */
public class CrawlStateTracker {
    
    long startTimeSeconds;
    Map<Integer, Integer> numCrawledMap;
    Map<Integer, Boolean> threadStatuses;
    int localWorkerIndex; // worker index of current instance
    int targetNumCrawled; // number of total documents to be crawled
    int numMessagesReceived; // number of sqs msgs received throughout crawl
    int numRejectedDuplicateURL; // number of urls rejected due to already being processed
    int numRejectedRobotsUnfetchable; // number of failed robots.txt GET requests
    int numRejectedRobotsRules; // number of urls rejected due to robots.txt restrictiosn
    int numDelayed; // number of urls temporarily rejected due to robots.txt crawl delay
    int numRejectedSize; // number of urls rejected due to page content being over size threshold
    int numRejectedHeaderUnsuitable; // number of urls rejected due to unsupported page MIME type
    int numRejectedContentIrrelevant; // number of urls rejected by content filter due to language or missing keywords
    int numRejectedContentSeen; // number of urls rejected due to content already being seen on other pages
    int numLocallyIndexed; // number of docs already downloaded
    int numRejectedHeaderUnfetchable; // number of failed HEAD requests
    int numRejectedContentUnfetchable; // number of failed content GET requests
    int numRobotTimeouts; // number of timed out robots.txt GET requests
    int numHeaderTimeouts; // number of timed out HEAD requests
    int numContentTimeouts; // number of timed out content GET requests
    int numUnhashableHosts; // number of urls which could not be hashed (debug only - should be 0)
    int numRejectedBadDomain;
    boolean endCrawl; // flag for ending crawl
    String syncObj = "HARRY POTTER"; // synchronization object used for coordinating among threads
    
    public synchronized void incNumRejectedHeaderUnfetchable() {
        numRejectedHeaderUnfetchable++;
    }
    
    public synchronized void incNumRejectedContentUnfetchable() {
        numRejectedContentUnfetchable++;
    }
    
    public synchronized void incNumMessagesReceived() {
        numMessagesReceived++;
    }
    
    public synchronized void incNumRejectedDuplicateURL() {
        numRejectedDuplicateURL++;
    }
    
    public synchronized void incNumRejectedRobotsUnfetchable() {
        numRejectedRobotsUnfetchable++;
    }
    
    public synchronized void incNumRejectedRobotsRules() {
        numRejectedRobotsRules++;
    }
    
    public synchronized void incNumDelayed() {
        numDelayed++;
    }
    
    public synchronized void incNumRejectedSize() {
        numRejectedSize++;
    }
    
    public synchronized void incNumRejectedHeaderUnsuitable() {
        numRejectedHeaderUnsuitable++;
    }
    
    public synchronized void incNumRejectedContentSeen() {
        numRejectedContentSeen++;
    }
    
    public synchronized void incNumLocallyIndexed() {
        numLocallyIndexed++;
        numCrawledMap.put(localWorkerIndex, numLocallyIndexed);
    }
    
    public synchronized void incNumRobotTimeouts() {
        numRobotTimeouts++;
    }
    
    public synchronized void incNumHeaderTimeouts() {
        numHeaderTimeouts++;
    }
    
    public synchronized void incNumContentTimeouts() {
        numContentTimeouts++;
    }
    
    public synchronized void incNumUnhashableHosts() {
        numUnhashableHosts++;
    }
    
    public synchronized void incNumRejectedContentIrrelevant() {
        numRejectedContentIrrelevant++;
    }
    
    public synchronized void incNumRejectedBadDomain() {
        numRejectedBadDomain++;
    }
    
    public synchronized void setThreadStatus(int threadId, boolean active) {
        threadStatuses.put(threadId, active);
        if (!active) {
            synchronized (syncObj) {
                syncObj.notifyAll();
            }
        }
    }
    
    public void waitUntilWithinThreshold(ConcurrentLinkedQueue<String> urlQueue, int thresh) {
        synchronized (syncObj) {
            try {
                while (urlQueue.size() > thresh) {
                    syncObj.wait();
                }
            } catch (InterruptedException e) { /*squelch*/ }
        }
    }
    
    public void waitUntilThreadsInactive() {
        synchronized (syncObj) {
            try {
                while (!areThreadsInactive()) {
                    syncObj.wait();
                }
            } catch (InterruptedException e) { /*squelch*/ }
        }
    }
    
    public synchronized boolean areThreadsInactive() {
        for (Integer i : threadStatuses.keySet()) {
            if (threadStatuses.get(i)) {
                return false;
            }
        }
        return true;
    }

    
    public CrawlStateTracker(int localWorkerIndex, int targetNumCrawled, int numLocallyIndexed) {
        this.localWorkerIndex = localWorkerIndex;
        this.targetNumCrawled = targetNumCrawled;
        numCrawledMap = new HashMap<>();
        threadStatuses = new HashMap<>();
        numCrawledMap.put(localWorkerIndex, 0);
        numMessagesReceived = 0;
        numRejectedDuplicateURL = 0;
        numRejectedRobotsUnfetchable = 0;
        numRejectedRobotsRules = 0;
        numDelayed = 0;
        numRejectedSize = 0;
        numRejectedHeaderUnsuitable = 0;
        numRejectedContentSeen = 0;
        this.numLocallyIndexed = numLocallyIndexed;
        numRejectedHeaderUnfetchable = 0;
        numRejectedContentUnfetchable = 0;
        numRobotTimeouts = 0;
        numHeaderTimeouts = 0;
        numContentTimeouts = 0;
        numUnhashableHosts = 0;
        numRejectedContentIrrelevant = 0;
        numRejectedBadDomain = 0;
        startTimeSeconds = System.currentTimeMillis() / 1000;
        endCrawl = false;
        
    }
    
    public synchronized void updateNumIndexed(int workerIndex, int numCrawled) {
        numCrawledMap.put(workerIndex, numCrawled);
    }
    
    public synchronized int getLocalNumIndexed() {
        return numCrawledMap.get(localWorkerIndex);
    }
    
    public synchronized boolean isTargetReached() {
        int totalNumCrawled = 0;
        for (Integer workerIndex : numCrawledMap.keySet()) {
            totalNumCrawled += numCrawledMap.get(workerIndex);
        }
        return totalNumCrawled >= targetNumCrawled;
    }
    
    public synchronized double getAvgIndexedPerSecond() {
        long currentTimeSeconds = System.currentTimeMillis() / 1000;
        long timeDiff = Math.max(1,  currentTimeSeconds - startTimeSeconds);
        return ((double) numLocallyIndexed) / ((double) timeDiff);
    }
    
    public synchronized String getLocalTrackState() {
        long currentTimeSeconds = System.currentTimeMillis() / 1000;
        long timeDiff = currentTimeSeconds - startTimeSeconds;
        return "Local Crawl State: " +
        "\nnumLocallyIndexed: " + numLocallyIndexed +
        "\nnumMessagesReceived: " + numMessagesReceived +
        "\nnumRejectedDuplicateURL: " + numRejectedDuplicateURL +
        "\nnumRejectedRobotsUnfetchable: " + numRejectedRobotsUnfetchable +
        "\nnumRejectedRobotsRules: " + numRejectedRobotsRules +
        "\nnumDelayed: " + numDelayed +
        "\nnumRejectedSize: " + numRejectedSize +
        "\nnumRejectedHeaderUnsuitable: " + numRejectedHeaderUnsuitable +
        "\nnumRejectedContentSeen: " + numRejectedContentSeen +
        "\nnumRejectedHeaderUnfetchable: " + numRejectedHeaderUnfetchable +
        "\nnumRejectedContentUnfetchable: " + numRejectedContentUnfetchable +
        "\nTime elapsed: " + timeDiff +
        "\nnumRobotTimeouts: " + numRobotTimeouts +
        "\nnumHeaderTimeouts: " + numHeaderTimeouts +
        "\nnumContentTimeouts: " + numContentTimeouts +
        "\nnumUnhashableHosts: " + numUnhashableHosts +
        "\nnumRejectedContentIrrelevant: " + numRejectedContentIrrelevant +
        "\nAvg Per Second: " + getAvgIndexedPerSecond();
    }
    
    public synchronized void endCrawl() {
        endCrawl = true;
    }
    
    public synchronized boolean isCrawlEnded() {
        return endCrawl;
    }
    

}
