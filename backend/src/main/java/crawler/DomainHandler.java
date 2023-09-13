package crawler;

import java.io.Serializable;

import crawlercommons.robots.SimpleRobotRules;

/**
 * Class for storing robots.txt records and metadata about a particular domain
 * @author Daniel
 *
 */
public class DomainHandler implements Serializable {
    
    private static final long serialVersionUID = -5165028078863117049L;
    long lastAccessedMillis;
    SimpleRobotRules rules;
    boolean isInvalid;
    int numSuccess;
    int numFail;
    
    public DomainHandler(SimpleRobotRules rules, boolean isInvalid) {
        this.rules = rules;
        this.isInvalid = isInvalid;
        updateLastAccessed();
        numSuccess = 0;
        numFail = 0;
    }
    
    public DomainHandler(SimpleRobotRules rules) {
        this(rules, false);
    }
    
    public DomainHandler(boolean isInvalid) {
        this(null, true);
    }
    
    /**
     * Returns number of times an HTTP query to this domain has been successful
     */
    public int getNumSuccess() {
        return numSuccess;
    }

    /**
     * Returns number of times an HTTP query to this domain has failed
     */
    public int getNumFail() {
        return numFail;
    }
    
    /**
     * Increments number of times an HTTP query to this domain has been successful
     */
    public void incNumSuccess() {
        numSuccess++;
    }

    /**
     * Increments number of times an HTTP query to this domain has failed
     */
    public void incNumFail() {
        numFail++;
    }
    
    /**
     * Returns whether this object represents a failed attempt to retrieve a robots.txt record.
     * Failed attempts are still stored in the database so they do not need to be retried
     * @return
     */
    public boolean isInvalid() {
        return isInvalid;
    }
    
    public SimpleRobotRules getRobotRules() {
        return rules;
    }
    
    /**
     * Returns whether the crawl delay has elapsed since the last access time
     * @return
     */
    public boolean isCrawlDelayElapsed() {
        long currentTimeMillis = System.currentTimeMillis();
        long crawlDelayMillis = rules.getCrawlDelay() * 1000;
        return currentTimeMillis - lastAccessedMillis > crawlDelayMillis;
    }
    
    /**
     * Saves the current timestamp as the last time that the domain was accessed
     */
    public void updateLastAccessed() {
        lastAccessedMillis = System.currentTimeMillis();
    }
    
    /**
     * Parses the given url and returns just the protocol and domain
     * (without the port, query params, or file path)
     * @param url
     * @return
     */
    public static String getProtocolDomainString(URLInfo urlInfo) {
            String protocol = urlInfo.isSecure() ? "https://" : "http://";
            String hostName = urlInfo.getHostName();
            int qIndex = hostName.indexOf("?");
            if (qIndex != -1) {
                    hostName = hostName.substring(0, qIndex);
            }
            return protocol + hostName;
    }

}
