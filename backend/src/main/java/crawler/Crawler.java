package crawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import crawlercommons.robots.SimpleRobotRules;
import crawlercommons.robots.SimpleRobotRulesParser;

/**
 * Crawler worker thread (does the actual crawling)
 * @author Daniel
 *
 */
public class Crawler implements Runnable {
    final static Logger logger = LogManager.getLogger(Crawler.class);
    final static String ROBOTS_TXT_FILEPATH = "/robots.txt";
    
    int threadId;
    Storage storage;
    URLEmitter urlEmitter;
    CrawlStateTracker tracker;
    ConcurrentLinkedQueue<String> urlQueue;
    List<String> keywords;
    
    public Crawler(Storage storage, CrawlStateTracker tracker, URLEmitter urlEmitter, 
            ConcurrentLinkedQueue<String> urlQueue, List<String> keywords, int threadId) {
        this.storage = storage;
        this.urlEmitter = urlEmitter;
        this.tracker = tracker;
        this.urlQueue = urlQueue;
        this.threadId = threadId;
        this.keywords = keywords;
    }
    
    /**
     * Returns whether the given domain has a high ratio of failed requests
     */
    public boolean isBadDomain(URLInfo url) {
        double numSuccess = (double) storage.getNumSuccess(url);
        double numFail = (double) storage.getNumFail(url);
        double failRatio = numFail / (numFail + numSuccess);
        return numFail > CrawlMaster.minBadDomainFails && failRatio > CrawlMaster.minBadDomainFailRatio;
    }
    
    /**
     * Crawls the given url. Returns +1 if the page was downloaded, -1 if it was rejected,
     * and 0 if it was delayed.
     * @param urlStr
     * @param metadata
     */
    public int processURL(String urlStr) {
        checkURLInfo(urlStr);
        
        URLInfo url = new URLInfo(urlStr);
        
        // rejects a url if its domain has a high ratio of failed requests
        if (isBadDomain(url)) {
            logger.info("Bad domain: " + urlStr);
            return -1;
        }
        
        // rejects a url if it has already been seen before
        if (storage.isURLSeen(url)) {
            tracker.incNumRejectedDuplicateURL();
            logger.info("Url already seen: " + urlStr);
            return -1;
        }
        
        DomainHandler handler = loadOrFetchRobots(url);
        
        // rejects a url if its robots.txt file threw an error while parsing
        if (handler == null ) {
            logger.info("Unable to retrieve robots.txt for: " + url.toString() + " original url: " + urlStr);
            tracker.incNumRejectedRobotsUnfetchable();
            storage.incNumFail(url);
            return -1;
        }
        
        // rejects a url if the robots.txt rules forbid crawling it
        if (!handler.getRobotRules().isAllowed(urlStr)) {
            logger.info("Not allowed to crawl: " + url.toString());
            tracker.incNumRejectedRobotsRules();
            return -1;
        }
        
        // temporarily rejects a url if the crawl delay for its domain has not yet elapsed
        if (!handler.isCrawlDelayElapsed()) {
            logger.debug("Crawl delay not elapsed: " + url.toString());
            urlEmitter.emitURL(urlStr);
            tracker.incNumDelayed();
            return 0;
        }
        
        HeaderInfo headerInfo = getHeaderInfo(url);
        
        // rejects a url if HEAD request failed
        if (headerInfo == null) {
            logger.info("No header info for: " + url.toString());
            tracker.incNumRejectedHeaderUnfetchable();
            storage.incNumFail(url);
            return -1;
        }
        
        // rejects url if content length is too large or is absent
        if (headerInfo.getContentLength() > CrawlMaster.maxDocSizeBytes
                //|| headerInfo.getContentLength() < 0
                ) {
            logger.info("Oversize: " + headerInfo.getContentLength() + ": " + url.toString() );
            tracker.incNumRejectedSize();
            return -1;
        }
        
        // rejects url if its MIME content type is not supported
        if (!headerInfo.isIndexableContentType()) {
            logger.info("Header info indicates doc not suitable for crawling: " + url.toString());
            logger.debug("nonIndexable: " + !headerInfo.isIndexableContentType() );
            logger.debug("content length: " + headerInfo.getContentLength() );
            tracker.incNumRejectedHeaderUnsuitable();
            return -1;
        }
        
        String content = fetchContent(url);
        
        
        // rejects url if content could not be retrieved
        if (content == null) {
            logger.info("Content null for: " + urlStr);
            tracker.incNumRejectedContentUnfetchable();
            storage.incNumFail(url);
            return -1;
        }
        
        headerInfo.setContentLength(content.length());
        
        // rejects url if content is too large (if content-length header lied)
        if (content.length() > CrawlMaster.maxDocSizeBytes) {
            logger.info("Content too large after download (" + content.length() + ") for: " + urlStr);
            tracker.incNumRejectedSize();
            return -1;
        }
        
        // parse html and extract text content
        Document doc = Jsoup.parse(content);
        String textContent = doc.text();
        
        // reject url if text content has already been seen
        if (storage.isContentSeen(textContent)) {
            logger.info("Content already seen for: " + urlStr);
            tracker.incNumRejectedContentSeen();
            return -1;
        }
        
        // rejects url if page is irrelevant
        if (!filterContent(doc, textContent)) {
            tracker.incNumRejectedContentIrrelevant();
            logger.info("Content filter rejected for: " + urlStr);
            return -1;
        }
        
        // saves page content
        logger.info("Saving content for: " + urlStr);
        storage.saveDocument(url, headerInfo, content, textContent);
        tracker.incNumLocallyIndexed();
        storage.incNumSuccess(url);
        
        // extracts page links
        if (headerInfo.isCrawlableContentType()) {
            logger.debug("Scraping links from: " + urlStr);
            emitLinks(doc, urlStr);
        }
        
        return 1;
    }
    
    /**
     * Returns whether the document is relevant or not.
     * Rejects any docs with html-lang attribute other than "en"
     * Accepts docs with keyword count over threshold, and probabilistically accepts
     * docs with keyword count below threshold
     * @param doc
     * @param textContent
     * @return
     */
    public boolean filterContent(Document doc, String textContent) {
        Element taglang = doc.select("html").first();
        if (taglang != null && !taglang.attr("lang").equals("") 
                && !taglang.attr("lang").toLowerCase().startsWith("en")) {
            return false;
        }
        int distinctKeywords = 0;
        for (String keyword : keywords) {
            if (StringUtils.containsIgnoreCase(textContent, keyword)) {
                distinctKeywords++;
            }
            if (distinctKeywords >= CrawlMaster.filterKeywordThreshold) {
                break;
            }
        }
        double probabilityTrue = (((double) distinctKeywords) / ((double) CrawlMaster.filterKeywordThreshold)) + CrawlMaster.minProbability;
         
        return Math.random() >= 1.0 - probabilityTrue;
    }

    /**
     * Emits the extracted links to the global queue.
     * Verifies that the url has not already been emitted.
     * Optionally limits the number of urls emitted from a page.
     * @param doc
     * @param fromUrlStr
     */
    private void emitLinks(Document doc, String fromUrlStr) {
        List<String> links = extractLinks(doc, fromUrlStr);
        List<String> emissions = new ArrayList<>();
        URLInfo urlInfo;
        urlInfo = new URLInfo(fromUrlStr);
        storage.saveEdges(urlInfo, new BDBListWrapper(links));
        int i = 0;
        for (String url : links) {
            urlInfo = new URLInfo(url);
            if (!storage.isURLEmitted(urlInfo) && !storage.isURLSeen(urlInfo)) {
                emissions.add(url);
                storage.markURLEmitted(urlInfo);
                i++;
            }
            
            if (CrawlMaster.maxURLEmits > 0 && i > CrawlMaster.maxURLEmits) {break;}
        }
        
        urlEmitter.emitURLs(emissions);
    }

    /**
     * Parses the given content String as an html document with the given baseURL
     * (the baseURL can be overridden if the html contains a base tag).
     * @param content
     * @param baseURL
     * @return
     */
    public List<String> extractLinks(Document doc, String baseURL) {
            List<String> links = new ArrayList<>();            
            // override base uri if present
            Elements base = doc.select("base");
            if (!base.isEmpty()) {
                    baseURL = base.get(0).attr("href");
                    doc.setBaseUri(baseURL);
            }
            
            //get all links
            Elements hrefs = doc.select("a[href]");
            for (Element elt : hrefs) {
                    links.add(elt.attr("abs:href"));
            }
            return links;
    }
    
    
    /**
     * Sends an HTTP GET request to retrieve the page content as a string,
     * or null if the request failed
     * @param url
     * @return
     */
    private String fetchContent(URLInfo url) {
        try {
            long startTime = System.currentTimeMillis();
            HttpURLConnection conn = sendRequest(url.toString(), "GET");
            int responseCode = conn.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                logger.debug("HTTP error " + responseCode + " while fetching content for: " + url.toString());
                return null;
            }
            String content = getStreamContent(conn.getInputStream());
            conn.disconnect();
            long endTime = System.currentTimeMillis();
            logger.debug("Time fetching content (ms): " + (endTime - startTime));
            return content;
        }
        catch (SocketTimeoutException e) {
            logger.debug("SocketTimeoutException while fetching content for: " + url.toString());
            tracker.incNumContentTimeouts();
            return null;
        }
        catch (IOException e) {
            logger.debug("IOException while parsing content stream for: " + url.toString());
            return null;
        }
    }

    /**
     * Sends an HTTP HEAD request to the given url and returns a HeaderInfo 
     * object representing the returned data, or null if the request failed
     * @param url
     * @return
     */
    private HeaderInfo getHeaderInfo(URLInfo url) {
        try {
            long startTime = System.currentTimeMillis();
            HttpURLConnection conn = sendRequest(url.toString(), "HEAD");
            int responseCode = conn.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                logger.debug("HTTP error " + responseCode + " while getting header for: " + url.toString());
                return null;
            }
            String contentType = conn.getContentType();
            int contentLength = conn.getContentLength();
            long lastModified = conn.getLastModified();
            conn.disconnect();
            long endTime = System.currentTimeMillis();
            logger.debug("Time fetching headers (ms): " + (endTime - startTime));
            return new HeaderInfo(contentLength, contentType, lastModified);
        }
        catch (SocketTimeoutException e) {
            logger.debug("SocketTimeoutException while fetching header for: " + url.toString());
            tracker.incNumHeaderTimeouts();
            return null;
        }
        catch (IOException e) {
            logger.debug("IOException while parsing header info for: " + url.toString());
            return null;
        }
    }

    /**
     * Attempts to fetch the robots.txt record for the given url from local storage.
     * If not present, fetches the record from the web and saves it locally before returning it
     * wrapped within a DomainHandler object. If no robots.txt record is present for the domain,
     * assumes there are no restrictions and returns a dummy object which allows all paths.
     * @param url
     * @return
     */
    private DomainHandler loadOrFetchRobots(URLInfo url) {
        DomainHandler handler = storage.getRobots(url);
        if (handler != null) {
            if (handler.isInvalid()) {
                return null;
            }
            return handler;
        }
        logger.debug("Fetching robots.txt for: " + url.toString());
        String robotsURL = DomainHandler.getProtocolDomainString(url) + ROBOTS_TXT_FILEPATH;
        try {
            long startTime = System.currentTimeMillis();
            HttpURLConnection conn = sendRequest(robotsURL.toString(), "GET");
            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
                handler = getAllowAllRobots(robotsURL);
            }
            else if (responseCode != HttpURLConnection.HTTP_OK) {
                logger.debug("HTTP error " + responseCode + " while getting robots.txt for: " + url.toString());
                storage.saveRobots(url, new DomainHandler(true));
                return null;
            }
            else {
                String robotsContent = getStreamContent(conn.getInputStream());
                SimpleRobotRulesParser parser = new SimpleRobotRulesParser();
                SimpleRobotRules rules = parser.parseContent(robotsURL, robotsContent.getBytes(), conn.getContentType(), CrawlMaster.crawlerName);
                handler = new DomainHandler(rules);
            }
            conn.disconnect();
            storage.saveRobots(url, handler);
            long endTime = System.currentTimeMillis();
            logger.debug("Time fetching robots (ms): " + (endTime - startTime));
            return handler;
        }
        catch (SocketTimeoutException e) {
            logger.debug("SocketTimeoutException while fetching robots for: " + url.toString());
            tracker.incNumRobotTimeouts();
            storage.saveRobots(url, new DomainHandler(true));
            return null;
        }
        catch (IOException e) {
            logger.debug("IOException while parsing robots.txt for: " + url.toString());
            storage.saveRobots(url, new DomainHandler(true));
            return null;
        }
    }
    
    /**
     * Returns a dummy DomainHandler which allows crawling all paths and imposes no crawl delay
     * @param robotsURL
     * @return
     */
    public DomainHandler getAllowAllRobots(String robotsURL) {
        SimpleRobotRulesParser parser = new SimpleRobotRulesParser();
        SimpleRobotRules rules = parser.parseContent(robotsURL, "".getBytes(), "text/plain", CrawlMaster.crawlerName);
        rules.setCrawlDelay(0);
        return new DomainHandler(rules);
    }
    
    /**
     * Sends a request to the given url using the given method (attaching relevant header), and returns
     * the resulting connection object. Imposes a read timeout and connect timeout on the connection.
     * @param url
     * @param method
     * @return
     * @throws IOException
     * @throws HttpFetchException
     */
    public HttpURLConnection sendRequest(String url, String method) throws IOException {
            URL urlObj = new URL(url);
            HttpURLConnection conn = (HttpURLConnection) urlObj.openConnection();
            conn.setInstanceFollowRedirects(true);
            conn.setReadTimeout(CrawlMaster.readTimeoutMillis);
            conn.setConnectTimeout(CrawlMaster.connectTimeoutMillis);
            conn.setRequestMethod(method);
            conn.setRequestProperty("User-Agent", CrawlMaster.crawlerName);
            return conn;
    }
    
    /**
     * Returns the output of the given stream as a string.
     * @param s
     * @return
     * @throws IOException
     */
    public String getStreamContent(InputStream s) {
            if (s == null) {
                    return "";
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(s));
            StringBuilder builder = new StringBuilder();
            try {
                String line = reader.readLine();
                while (line != null) {
                    builder.append(line + "\r\n");
                    line = reader.readLine();
                }
                reader.close();
                return builder.toString();
            }
            catch (IOException e) {
                logger.debug("IOException while parsing input stream");
                return null;
            }
            
    }
    
    /**
     * Sanity check for making sure the given string can be parsed
     */
    public void checkURLInfo(String urlStr) {
        URLInfo url = new URLInfo(urlStr);
        if (url.getHostName().contains("#") || url.getHostName().contains("?") 
                || url.getHostName().contains(":")) {
            logger.debug("Problem url: " + urlStr + " with host name: " + url.getHostName());
        }
    }


    /**
     * Polls the url queue and processes extracted items
     */
    @Override
    public void run() {
        String url;
        int crawlResult;
        try {
            while (!Thread.interrupted()) {
                url = urlQueue.poll();
                if (url == null) {
                    synchronized (urlQueue) {
                        urlQueue.wait();
                    }
                }
                else {
                    tracker.setThreadStatus(threadId, true);
                    try {
                        crawlResult = processURL(url);
                        if (crawlResult != 0) {
                            storage.markURLSeen(new URLInfo(url));
                        }
                    }
                    catch (Exception e) {
                        logger.error("Crawler threw exception: " + e.toString());
                        e.printStackTrace();
                    }
                    tracker.setThreadStatus(threadId, false);
                }
            }
        }
        catch (InterruptedException e) {
            return;
        }
    }

}
