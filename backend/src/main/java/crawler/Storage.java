package crawler;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

//import com.google.common.hash.BloomFilter;
//import com.google.common.hash.Funnels;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * Berkeley-DB backed storage class for documents, edge links, robots.txt records, and other
 * miscellaneous data.
 * @author Daniel
 *
 */
public class Storage {
    final static int NUM_HASHING_BLOCKS = 5; 
    
    final static String CLASS_CATALOG_DB_NAME = "CATALOG_DB";
    final static String DOCUMENTS_DB_NAME = "DOCUMENTS_DB";
    final static String ROBOTS_DB_NAME = "ROBOTS_DB";
    final static String CRAWLED_CONTENT_DB_NAME = "CRAWLED_DB";
    final static String EDGES_DB_NAME = "EDGES_DB";
    final static String SEEN_URLS_DB_NAME = "SEEN_URLS_DB";
    final static String EMITTED_URLS_DB_NAME = "EMITTED_URLS_DB";
    
    String dir;
    
    Map<String, CrawlerDocument> documents;
    Map<String, Boolean> seenURLs;
    Map<String, Boolean> emittedURLs;
    Map<String, String> crawledContent;
    Map<String, BDBListWrapper> edges;
    Map<String, DomainHandler> robots;
    
    StoredClassCatalog catalog;
    Environment env;
    
    Database documentsDB;
    Database robotsDB;
    Database crawledContentDB;
    Database edgesDB;
    Database seenURLsDB;
    Database emittedURLsDB;
    
    
    public Storage(String dir) {
        this.dir = dir;
        createIfNotExists();
        open();
    }
    
    /**
     * Initializes database objects and storage-backed maps
     */
    public void open() {
        EnvironmentConfig eConfig = new EnvironmentConfig();
        eConfig.setAllowCreate(true);
        File dbFile = new File(dir);
        env = new Environment(dbFile, eConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        
        Database catalogDB = env.openDatabase(null, CLASS_CATALOG_DB_NAME, dbConfig);
        catalog = new StoredClassCatalog(catalogDB);
        
        documentsDB = env.openDatabase(null, DOCUMENTS_DB_NAME, dbConfig);
        documents = getDBMap(documentsDB, String.class, CrawlerDocument.class);
        
        robotsDB = env.openDatabase(null, ROBOTS_DB_NAME, dbConfig);
        robots = getDBMap(robotsDB, String.class, DomainHandler.class);
        
        crawledContentDB = env.openDatabase(null, CRAWLED_CONTENT_DB_NAME, dbConfig);
        crawledContent = getDBMap(crawledContentDB, String.class, String.class);
        
        seenURLsDB = env.openDatabase(null, SEEN_URLS_DB_NAME, dbConfig);
        seenURLs = getDBMap(seenURLsDB, String.class, Boolean.class);
        
        emittedURLsDB = env.openDatabase(null, EMITTED_URLS_DB_NAME, dbConfig);
        emittedURLs = getDBMap(emittedURLsDB, String.class, Boolean.class);
        
        edgesDB = env.openDatabase(null, EDGES_DB_NAME, dbConfig);
        edges = getDBMap(edgesDB, String.class, BDBListWrapper.class);
        
    }
    
    /**
     * Creates the storage directory if it doesn't exist
     */
    public synchronized void createIfNotExists() {
        File d = new File(dir);
        if (!d.exists()) {
            d.mkdir();
        }
    }
    
    protected <K, V> StoredSortedMap<K, V> getDBMap(Database db, Class<K> keyClass, Class<V> valueClass) {
        EntryBinding<K> keyBinding = new SerialBinding<K>(catalog, keyClass);
        EntryBinding<V> valueBinding = new SerialBinding<V>(catalog, valueClass);
        return new StoredSortedMap<K, V>(db, keyBinding, valueBinding, true);
    }
    
    /**
     * Saves an adjacency list where the page corresponding to fromURL contains links to all
     * the pages in toURLs
     * @param fromURL
     * @param l
     */
    public synchronized void saveEdges(URLInfo fromURL, BDBListWrapper toURLs) {
        edges.put(fromURL.toString(), toURLs);
    }
    
    /**
     * Saves the given document and its metadata, and stores the hash of the pages text
     * for looking up duplicate content
     * @param url the url of the document
     * @param headerInfo the info retrieved from the initial HEAD request
     * @param content the html source code of the page
     * @param textContent the extracted text content of the page (for hashing)
     */
    public synchronized void saveDocument(URLInfo url, HeaderInfo headerInfo, String content, 
            String textContent) {
        if (isURLIndexed(url)) {
            return; // TODO consider replacing doc rather than refusing to update
        }
        CrawlerDocument doc = new CrawlerDocument(url.toString(), content, 
                headerInfo.getContentType(), headerInfo.getLastModified());
        documents.put(url.toString(), doc);
        String contentHash = getContentHash(textContent);
        
        crawledContent.put(contentHash, url.toString());
    }
    
    /**
     * Returns whether the page corresponding to the given url has been downloaded and stored
     * @param url
     * @return
     */
    public synchronized boolean isURLIndexed(URLInfo url) {
        return documents.containsKey(url.toString());
    }
    
    /**
     * Saves the robots.txt rules for the host name corresponding to the given url
     */
    public synchronized void saveRobots(URLInfo url, DomainHandler handler) {
        if (hasRobotsEntry(url)) {
            return;
        }
        String hostName = url.getHostName();
        robots.put(hostName, handler);
    }
    
    /**
     * Returns the page's outgoing links, or null if not present.
     * @param toURL
     * @return
     */
    public synchronized List<String> getEdges(String fromURL) {
        BDBListWrapper w = edges.get(fromURL.toString());
        if (w != null) {
            return w.fromEdges;
        }
        return null;
    }
    
    /**
     * Retrieves the robots.txt record corresponding to the host name of the given url,
     * or null if not present.
     * @param url
     * @return
     */
    public synchronized DomainHandler getRobots(URLInfo url) { // keyed on domain
        String hostName = url.getHostName();
        DomainHandler r = robots.get(hostName);
        if (r == null) {
            return null;
        }
        r.updateLastAccessed();
        robots.replace(hostName, r);
        return r;
    }
    
    /**
     * Returns whether a robots.txt entry has been stored for the
     * host name corresponding to the given url
     * @param url
     * @return
     */
    public synchronized boolean hasRobotsEntry(URLInfo url) {
        String hostName = url.getHostName();
        return robots.containsKey(hostName);
    }
    
    /**
     * Returns whether the given text content has already been seen by the crawler
     * @param textContent
     * @return
     */
    public synchronized boolean isContentSeen(String textContent) {
        String contentHash = getContentHash(textContent);
        return crawledContent.containsKey(contentHash);
    }
    
    /**
     * Returns whether the given url has already been seen (though potentially not downloaded)
     *  by the crawler
     * @param url
     * @return
     */
    public synchronized boolean isURLSeen(URLInfo url) {
        return seenURLs.containsKey(url.toString());
    }
    
    /**
     * Marks the url as seen by the crawler so it is not processed again
     * @param url
     */
    public synchronized void markURLSeen(URLInfo url) {
        seenURLs.put(url.toString(), true);
    }
    
    /**
     * Gets the html source code corresponding to to the given url
     * @param url
     * @return
     */
    public synchronized String getContent(URLInfo url) {
        CrawlerDocument doc = documents.get(url.toString());
        if (doc == null) {
            return null;
        }
        return doc.getContent();
    }
    
    /**
     * Returns the MD5 hash of the given text content (with all spaces removed first)
     * @param textContent
     * @return
     */
    public String getContentHash(String textContent) {
        textContent = textContent.replaceAll("\\s", "");
        return CrawlerDocument.computeMD5Hash(textContent);
    }
    
    /**
     * Gets the total number of stored docs
     * @return
     */
    public synchronized int getNumDocs() {
        return documents.size();
    }
    
    /**
     * Provides an iterator over the stored documents
     * @return
     */
    public synchronized Iterator<CrawlerDocument> getDocIterator() {
        return documents.values().iterator();
    }
    
    /**
     * Gets the total number of stored adjacency list entries (should be equal to the number of docs)
     * @return
     */
    public synchronized int getNumEdges() {
        return edges.size();
    }
    
    /**
     * Provides an iterator over the <url, outgoing edge list> pairs
     * @return
     */
    public synchronized Iterator<Entry<String, BDBListWrapper>> getEdges() {
        return edges.entrySet().iterator();
    }
    
    /**
     * Marks the url as emitted by the crawler so it is not emitted again
     * @param url
     */
    public synchronized void markURLEmitted(URLInfo url) {
        if (!isURLEmitted(url)) {
            emittedURLs.put(url.toString(), true);
        }
    }
    
    /**
     * Returns whether the given url has already been emitted by the crawler instance
     * @param url
     * @return
     */
    public synchronized boolean isURLEmitted(URLInfo url) {
        return emittedURLs.containsKey(url.toString());
    }
    
    /**
     * Returns the number of times that a query to the domain corresponding to the given url
     * has been successful
     * @param url
     * @return
     */
    public synchronized int getNumSuccess(URLInfo url) {
        String hostName = url.getHostName();
        DomainHandler r = robots.get(hostName);
        if (r == null) {
            return 0;
        }
        return r.getNumSuccess();
    }

    /**
     * Returns the number of times that a query to the domain corresponding to the given url
     * has failed
     * @param url
     * @return
     */
    public synchronized int getNumFail(URLInfo url) {
        String hostName = url.getHostName();
        DomainHandler r = robots.get(hostName);
        if (r == null) {
            return 0;
        }
        return r.getNumFail();
    }
    
    /**
     * Increments the number of times that a query to the domain corresponding to the given url
     * has been successful
     * @param url
     * @return
     */
    public synchronized void incNumSuccess(URLInfo url) {
        String hostName = url.getHostName();
        DomainHandler r = robots.get(hostName);
        if (r == null) {
            return;
        }
        r.incNumSuccess();
        robots.replace(hostName, r);
        
    }

    /**
     * Increments the number of times that a query to the domain corresponding to the given url
     * has failed
     * @param url
     * @return
     */
    public synchronized void incNumFail(URLInfo url) {
        String hostName = url.getHostName();
        DomainHandler r = robots.get(hostName);
        if (r == null) {
            return;
        }
        r.incNumFail();
        robots.replace(hostName, r);
    }
        
    /**
     * Closes all database objects
     */
    public synchronized void close() {
        documentsDB.close();
        robotsDB.close();
        crawledContentDB.close();
        edgesDB.close();
        seenURLsDB.close();
        emittedURLsDB.close();
        
        catalog.close();
        env.close();
    }
    
    /**
     * Closes and reopens all database objects to force a flush to the disk
     */
    public synchronized void flush() {
        close();
        open();
    }

}
