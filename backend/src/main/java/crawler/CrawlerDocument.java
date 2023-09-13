package crawler;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Class used for representing crawled pages and associated data/metadata
 * @author Daniel
 *
 */
public class CrawlerDocument implements Serializable {
    private static final long serialVersionUID = -1751559966766923506L;
    String url;
    String content;
    String contentType;
    long lastCrawledSeconds;
    long lastModifiedSeconds;
    
    public CrawlerDocument(String url, String content, String contentType, long lastModified) {
        this.url = url;
        this.content = content;
        this.contentType = contentType;
        this.lastModifiedSeconds = lastModified;
        updateLastCrawled();
    }
    
    public String getURL() {
        return url;
    }
    
    public String getContent() {
        return content;
    }
    
    public String getContentType() {
        return contentType;
    }
    
    public long getLastModified() {
        return lastModifiedSeconds;
    }

    public static String computeMD5Hash(String input) {
        try {
            MessageDigest digestGen = MessageDigest.getInstance("MD5");
            digestGen.update(input.getBytes());
            byte[] digest = digestGen.digest();
            BigInteger digestInt = new BigInteger(1, digest);
            return digestInt.toString(16);
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    }
    
    public String getMD5Hash() {
        return computeMD5Hash(content);
    }
    
    public void updateLastCrawled() {
        lastCrawledSeconds = System.currentTimeMillis() / 1000;
    }
    
    public long getLastCrawled() {
        return lastCrawledSeconds;
    }
    
    public void setLastCrawled(long t) {
        lastCrawledSeconds = t;
    }
    
    @Override
    public String toString() {
        return "url: " + url +
                "\ncontent: " + content.substring(0, Math.min(content.length(), 100)) +
                "\ncontentLength: " + content.length() +
                "\ncontentType: " + contentType +
                "\nlastModified: " + lastModifiedSeconds;
    }

}
