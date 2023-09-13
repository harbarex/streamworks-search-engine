package crawler;

/**
 * Stores information returned by a HEAD request
 */
public class HeaderInfo {
    int contentLength;
    String contentType;
    long lastModified;
    
    public HeaderInfo(int contentLength, String contentType, long lastModified) {
        this.contentLength = contentLength;
        this.contentType = contentType;
        this.lastModified = lastModified;
    }
    
    public int getContentLength() {
        return contentLength;
    }
    
    public void setContentLength(int len) {
        contentLength = len;
    }
    
    public String getContentType() {
        return contentType;
    }
    
    public long getLastModified() {
        return lastModified;
    }
    
    /**
     * Returns whether the content type is one of the content types we should index
     */
    public boolean isIndexableContentType() {
        return contentType != null && (contentType.strip().toLowerCase().startsWith("text/html"));   
    }

    public boolean isCrawlableContentType() {
        return contentType != null && (contentType.strip().toLowerCase().startsWith("text/html"));
    }
}
