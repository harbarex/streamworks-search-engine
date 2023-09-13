import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import crawler.URLInfo;

class URLInfoTests {

    @Test
    void testPathQueryAnchor() {
        String url = "http://google.com/path/to/here?param1=val1&param2=val2#place";
        URLInfo urlInfo = new URLInfo(url);
        assertEquals(false, urlInfo.isSecure());
        assertEquals("google.com", urlInfo.getHostName());
        assertEquals(80, urlInfo.getPortNo());
        assertEquals("/path/to/here", urlInfo.getFilePath());
        assertEquals("?param1=val1&param2=val2", urlInfo.getQuery());
        assertEquals("#place", urlInfo.getAnchor());
        assertEquals("http://google.com:80/path/to/here?param1=val1&param2=val2#place", urlInfo.toStringWithAnchor());
    }
    
    @Test
    void testPathQuery() {
        String url = "http://google.com/path/to/here?param1=val1&param2=val2";
        URLInfo urlInfo = new URLInfo(url);
        assertEquals(false, urlInfo.isSecure());
        assertEquals("google.com", urlInfo.getHostName());
        assertEquals(80, urlInfo.getPortNo());
        assertEquals("/path/to/here", urlInfo.getFilePath());
        assertEquals("?param1=val1&param2=val2", urlInfo.getQuery());
        assertEquals("", urlInfo.getAnchor());
        assertEquals("http://google.com:80/path/to/here?param1=val1&param2=val2", urlInfo.toStringWithAnchor());
    }
    
    @Test
    void testPathAnchor() {
        String url = "http://google.com/path/to/here#place";
        URLInfo urlInfo = new URLInfo(url);
        assertEquals(false, urlInfo.isSecure());
        assertEquals("google.com", urlInfo.getHostName());
        assertEquals(80, urlInfo.getPortNo());
        assertEquals("/path/to/here", urlInfo.getFilePath());
        assertEquals("", urlInfo.getQuery());
        assertEquals("#place", urlInfo.getAnchor());
        assertEquals("http://google.com:80/path/to/here#place", urlInfo.toStringWithAnchor());
    }
    
    @Test
    void testQueryAnchor() {
        String url = "http://google.com?param1=val1&param2=val2#place";
        URLInfo urlInfo = new URLInfo(url);
        assertEquals(false, urlInfo.isSecure());
        assertEquals("google.com", urlInfo.getHostName());
        assertEquals(80, urlInfo.getPortNo());
        assertEquals("", urlInfo.getFilePath());
        assertEquals("?param1=val1&param2=val2", urlInfo.getQuery());
        assertEquals("#place", urlInfo.getAnchor());
        assertEquals("http://google.com:80?param1=val1&param2=val2#place", urlInfo.toStringWithAnchor());
    }
    
    @Test
    void testQuery() {
        String url = "http://google.com?param1=val1&param2=val2";
        URLInfo urlInfo = new URLInfo(url);
        assertEquals(false, urlInfo.isSecure());
        assertEquals("google.com", urlInfo.getHostName());
        assertEquals(80, urlInfo.getPortNo());
        assertEquals("", urlInfo.getFilePath());
        assertEquals("?param1=val1&param2=val2", urlInfo.getQuery());
        assertEquals("", urlInfo.getAnchor());
        assertEquals("http://google.com:80?param1=val1&param2=val2", urlInfo.toStringWithAnchor());
    }
    
    @Test
    void testAnchor() {
        String url = "http://google.com#place";
        URLInfo urlInfo = new URLInfo(url);
        assertEquals(false, urlInfo.isSecure());
        assertEquals("google.com", urlInfo.getHostName());
        assertEquals(80, urlInfo.getPortNo());
        assertEquals("", urlInfo.getFilePath());
        assertEquals("", urlInfo.getQuery());
        assertEquals("#place", urlInfo.getAnchor());
        assertEquals("http://google.com:80#place", urlInfo.toStringWithAnchor());
    }
    
    @Test
    void testTrailingSlash() {
        String url = "https://cur8able.com/";
        URLInfo urlInfo = new URLInfo(url);
        assertEquals(true, urlInfo.isSecure());
        assertEquals("cur8able.com", urlInfo.getHostName());
        assertEquals(443, urlInfo.getPortNo());
        assertEquals("/", urlInfo.getFilePath());
        assertEquals("", urlInfo.getQuery());
        assertEquals("", urlInfo.getAnchor());
        assertEquals("https://cur8able.com:443/", urlInfo.toStringWithAnchor());
        
    }

}
