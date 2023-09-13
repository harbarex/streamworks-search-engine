package edu.upenn.cis.cis455.storage;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.io.File;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicInteger;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.collections.StoredEntrySet;
import com.sleepycat.collections.StoredSortedMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.cis455.storage.StorageInterface;
import edu.upenn.cis.cis455.storage.User;
import edu.upenn.cis.cis455.storage.Document;
import edu.upenn.cis.cis455.storage.ContentDigest;
import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import edu.upenn.cis.cis455.storage.Channel;

public class CrawlerDatabase implements StorageInterface {

    final static Logger logger = LogManager.getLogger(CrawlerDatabase.class);

    // The field for the Crawler Database Environment
    private Environment env;

    // The default names for three DBs in the Crawler Database Environment
    private static final String crawlerClassCatalog = "crawler_class_catalog";
    private static final String userDBName = "user_db";
    private static final String docDBName = "doc_db";
    private static final String contentDigestDBName = "content_digest_db";
    private static final String channelDBName = "channel_db";
    private static final String urlFrontierDBName = "url_frontier_db";

    private static final String docIDtoDocDBName = "id_to_doc_db";

    // for communication between crawler & stormlite
    private static final String stormStateDBName = "storm_state_db";
    private static final String docCounterDBName = "doc_counter_db";

    // The fields for four DBs
    private StoredClassCatalog crawlerCatalog;
    private Database userDB;
    private Database docDB;
    private Database contentDigestDB; // this is used to verify content seen or not
    private Database channelDB; // for channel info (MS2)
    private Database urlFrontierDB; // serve as a url queue
    private Database stormStateDB; // serve as an array of the spout & bolt state
    private Database docCounterDB;

    private Database docIDtoDocDB;

    // The collections to access, remove and retrieve items from corresponding DBs
    private StoredSortedMap userMap;
    private StoredSortedMap docMap;
    private StoredSortedMap contentDigestMap;
    private StoredSortedMap channelMap;
    private StoredSortedMap urlFrontier;
    private StoredSortedMap stormState;
    private StoredSortedMap docCounter;

    private StoredSortedMap docIDtoDoc;

    // Atomic integers for IDs
    private AtomicInteger nextUserID;
    private AtomicInteger nextDocID;
    private AtomicInteger nextFrontierID;

    /**
     * Constructor
     * 
     * @param directory : [String], the file location for database environment
     */
    public CrawlerDatabase(String directory) throws DatabaseException {

        try {

            // initiate the environment
            logger.debug("(CrawlerDB) Opening Env in " + directory);

            EnvironmentConfig envConfig = new EnvironmentConfig();

            // setTransactional to true for persistent storage
            // otherwise, the data won't be stored
            envConfig.setAllowCreate(true);
            envConfig.setTransactional(true);

            this.env = new Environment(new File(directory), envConfig);

            // two databases in the environment
            DatabaseConfig dbConfig = new DatabaseConfig();

            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(true);

            // catalog
            Database catalogDB = env.openDatabase(null, crawlerClassCatalog, dbConfig);
            this.crawlerCatalog = new StoredClassCatalog(catalogDB);

            // 1. user database
            this.userDB = env.openDatabase(null, userDBName, dbConfig);
            this.initUserMap();

            // 2. crawler database
            this.docDB = env.openDatabase(null, docDBName, dbConfig);
            this.docIDtoDocDB = env.openDatabase(null, docIDtoDocDBName, dbConfig);
            this.initDocDB();

            // 3. content digest
            this.contentDigestDB = env.openDatabase(null, contentDigestDBName, dbConfig);
            this.initContentDigestDB();

            // 4. channel db
            this.channelDB = env.openDatabase(null, channelDBName, dbConfig);
            this.initChannelDB();

            // 5. frontier db (for StormLite)
            this.urlFrontierDB = env.openDatabase(null, urlFrontierDBName, dbConfig);
            this.initURLFrontierDB();

            // 6. & 7. for communication between master crawler & spouts & bolts
            this.stormStateDB = env.openDatabase(null, stormStateDBName, dbConfig);
            this.initStormStateDB();

            this.docCounterDB = env.openDatabase(null, docCounterDBName, dbConfig);
            this.initDocCounterDB();

        } catch (DatabaseException de) {

            throw de;

        }

    }

    /**
     * Initialize the user DB
     * with corresponding bindings (Key: String, Value: User).
     * 
     */
    private void initUserMap() {

        // create the key & value bindings for User DB
        EntryBinding userKeyBinding = new SerialBinding(this.crawlerCatalog, String.class);
        EntryBinding userValueBinding = new SerialBinding(this.crawlerCatalog, User.class);

        // initialize
        this.userMap = new StoredSortedMap(this.userDB, userKeyBinding, userValueBinding, true);

        // set up the next UserID
        int totalUsers = this.userMap.size();
        nextUserID = new AtomicInteger(totalUsers);

        logger.debug("(CrawlerDB) initialize user map => current total: " + totalUsers);

    }

    /**
     * Initialize the document DB
     * with corresponding bindings (Key: String, Value: Document).
     * 
     */
    private void initDocDB() {

        // create the key & value bindings
        EntryBinding docKeyBinding = new SerialBinding(this.crawlerCatalog, String.class);
        EntryBinding docValueBinding = new SerialBinding(this.crawlerCatalog, Document.class);

        // initialize
        this.docMap = new StoredSortedMap(this.docDB, docKeyBinding, docValueBinding, true);

        this.docIDtoDoc = new StoredSortedMap<>(this.docIDtoDocDB,
                new SerialBinding(this.crawlerCatalog, Integer.class),
                new SerialBinding(this.crawlerCatalog, Document.class), true);

        // set up the next docID
        int totalDocs = this.docMap.size();
        nextDocID = new AtomicInteger(totalDocs);

        // init id 2 doc

        logger.debug("(CrawlerDB) initialize doc map => current total: " + nextDocID);

    }

    /**
     * Initialize the content digest DB
     * with corresponding bindings (Key: digest (MD5 hashed), Value: ContentDigest).
     * 
     */
    private void initContentDigestDB() {

        // create the key & value bindings
        EntryBinding digestKeyBinding = new SerialBinding(this.crawlerCatalog, String.class);
        EntryBinding digestValueBinding = new SerialBinding(this.crawlerCatalog, ContentDigest.class);

        // initialize
        this.contentDigestMap = new StoredSortedMap(this.contentDigestDB, digestKeyBinding, digestValueBinding, true);

        // clear all the contents in this db whenever start
        this.contentDigestMap.clear();

        logger.debug("(CrawlerDB) initialize content digest map => current total: " + this.contentDigestMap.size());

    }

    /**
     * (MS2)
     * Initialize the channel DB
     * 
     */
    private void initChannelDB() {

        // create bindings
        EntryBinding channelKeyBinding = new SerialBinding(this.crawlerCatalog, String.class);
        EntryBinding channelValueBinding = new SerialBinding(this.crawlerCatalog, Channel.class);

        // init
        this.channelMap = new StoredSortedMap(this.channelDB, channelKeyBinding, channelValueBinding, true);
        logger.debug("(CrawlerDB) initialize channel map => current total: " + this.channelMap.size());

    }

    /**
     * (MS2)
     * Frontier queue for StormLite framework.
     * 
     */
    private void initURLFrontierDB() {

        EntryBinding URLKeyBinding = new SerialBinding(this.crawlerCatalog, String.class);
        EntryBinding URLValueBinding = new SerialBinding(this.crawlerCatalog, URLInfo.class);

        this.urlFrontier = new StoredSortedMap(this.urlFrontierDB, URLKeyBinding, URLValueBinding, true);

        this.urlFrontier.clear();

        this.nextFrontierID = new AtomicInteger();

        logger.debug("(CrawlerDB) initialize url frontier => total: " + this.urlFrontier.size());

    }

    /**
     * (MS2)
     * Storm States for StormLite framework.
     * 
     */
    private void initStormStateDB() {

        EntryBinding keyBinding = new SerialBinding(this.crawlerCatalog, String.class);
        EntryBinding valueBinding = new SerialBinding(this.crawlerCatalog, Boolean.class);

        this.stormState = new StoredSortedMap(this.stormStateDB, keyBinding, valueBinding, true);

        this.stormState.clear();

        logger.debug("(CrawlerDB) initialize storm state");

    }

    /**
     * (MS2)
     * Doc Counter (for each crawler run) for StormLite framework.
     * 
     */
    private void initDocCounterDB() {

        EntryBinding keyBinding = new SerialBinding(this.crawlerCatalog, String.class);
        EntryBinding valueBinding = new SerialBinding(this.crawlerCatalog, Integer.class);

        this.docCounter = new StoredSortedMap(this.docCounterDB, keyBinding, valueBinding, true);

        this.docCounter.clear();

        logger.debug("(CrawlerDB) initialize document counter");

    }

    /**
     * Get current time as Date object
     * 
     * @return [Date]
     */
    public static Date getCurrentDate() {

        Calendar c = Calendar.getInstance();

        return c.getTime();

    }

    /**
     * Format an Date object and get its date string in http server format
     * 
     * @param date : [Date]
     * @return [String]
     */
    public static String formatDateToString(Date date) {

        // set up format and set it to GMT
        SimpleDateFormat dateF = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
        dateF.setTimeZone(TimeZone.getTimeZone("GMT"));

        return dateF.format(date);

    }

    /**
     * Try parse the date string into the correct format.
     * 
     * @param dateString : [String]
     *                   The date string from the request
     * @return Date [Date]
     */
    public static Date parseDate(String dateString) {

        try {

            SimpleDateFormat sourceFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
            sourceFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

            Date parsedDate = sourceFormat.parse(dateString);

            return parsedDate;

        } catch (ParseException pe) {

            logger.debug("(Thread " + Thread.currentThread().getId()
                    + " - CrawlerDB) This date string may not in the Response format!");

        }

        return null;
    }

    /**
     * Compare last visited date & last modified date
     * 
     * @param lastVisitedDate  : [String]
     *                         The date of last visit
     * @param lastModifiedDate : [String]
     *                         The date from the http response (Last-Modified Date)
     * @return [Boolean],
     */
    public static boolean modifiedSinceLastVisit(String lastVisitedDate, String lastModifiedDate) {

        SimpleDateFormat format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");

        try {

            // last visited date is before last modified => has been modified
            if (format.parse(lastVisitedDate).before(format.parse(lastModifiedDate))) {

                return true;

            }

        } catch (ParseException pe) {

            logger.debug("Get pased exception when comparing two date strings!");

            pe.printStackTrace();

        }

        // any error treat it as unmodified
        return false;

    }

    @Override
    public boolean isContentSeen(String digest) {

        if (this.contentDigestMap.containsKey(digest)) {

            logger.info("(Thread " + Thread.currentThread().getId() + " - CrawlerDB) Get seen content!");

            return true;

        }

        // either (content unseen) or (not seen in this run)
        return false;

    }

    @Override
    public void addContentDigest(String digest, String content) {

        ContentDigest newDigest = new ContentDigest(digest, content);

        this.contentDigestMap.put(digest, newDigest);

        logger.info("(Thread " + Thread.currentThread().getId() + " - CrawlerDB) Added a new digest: " + digest);

    }

    @Override
    public int getContentSeenCount() {

        return this.contentDigestMap.size();

    }

    /**
     * How many documents so far?
     */
    @Override
    public int getCorpusSize() {

        // nextDocID: the id for the incoming doc
        // (equals to the total number of docs in DB)
        return this.nextDocID.get();

    }

    /**
     * TODO: Check should fetch document or not.
     * (1) URL seen or not
     * (2) If seen, lastModified vs. lastTimeFetched
     */
    @Override
    public boolean shouldAddDocument(String url, String lastModifiedDate) {

        if (!this.docMap.containsKey(url)) {

            logger.info("(Thread " + Thread.currentThread().getId() + " - CrawlerDB) Get an unseen url: " + url);

            return true;

        }

        if ((lastModifiedDate != null) && (this.docMap.containsKey(url))) {

            // format lastModified
            Document existedDoc = (Document) this.docMap.get(url);
            String lastVisitedDate = existedDoc.getLastVisitedDate();

            // if unmodified after last visited
            if (modifiedSinceLastVisit(lastVisitedDate, lastModifiedDate)) {

                logger.info("(Thread " + Thread.currentThread().getId()
                        + " - CrawlerDB) Seen before but modified after last visited: " + url);

                return true;

            }

        }

        logger.info("(Thread " + Thread.currentThread().getId()
                + " - CrawlerDB) Seen before and un-modified: " + url);

        return false;

    }

    /**
     * Add a new document, getting its ID.
     * (TODO: check should verify exist or not)
     * Note: should verify seen or not before adding it.
     */
    @Override
    public int addDocument(String url, String documentContents) {

        // next doc id
        int docID = this.nextDocID.getAndIncrement();

        String current = formatDateToString(getCurrentDate());

        // Create new Document object
        Document newDoc = new Document(docID, url, documentContents, current);

        // store this user in database
        this.docMap.put(url, newDoc);

        // add docIDtoDoc as well
        this.docIDtoDoc.put(docID, newDoc);

        logger.info("(Thread " + Thread.currentThread().getId() + " - CrawlerDB) Add new document! ID: "
                + docID + " (url: " + url + ")");

        return docID;
    }

    /**
     * Retrieves a document's contents by URL
     */
    @Override
    public String getDocument(String url) {

        if (this.docMap.containsKey(url)) {

            Document retrievedDoc = (Document) this.docMap.get(url);

            return retrievedDoc.getContent();

        }

        return null;

    }

    /**
     * Check user exists or not
     */
    @Override
    public boolean userExists(String username) {

        if (this.userMap.containsKey(username)) {

            return true;

        }

        return false;

    }

    /**
     * Adds a user and returns an ID
     */
    @Override
    public int addUser(String username, String password) {

        // TODO: Check duplicated username (or allow duplicate ?!)
        // Note: No duplicate is allowed, if username is the same, it replaces ..

        // create user obj for this new user
        int userID = this.nextUserID.getAndIncrement();
        User newUser = new User(userID, username, password);

        // store this user in database
        this.userMap.put(username, newUser);

        logger.debug("(Thread " + Thread.currentThread().getId() + " - CrawlerDB) User added: " + newUser.toString());

        return userID;

    }

    /**
     * Tries to log in the user, or else throws a HaltException
     */
    @Override
    public boolean getSessionForUser(String username, String password) {

        if (this.userMap.containsKey(username)) {

            // check password (Explicitly cast it to User)
            User fetchedUser = (User) this.userMap.get(username);

            if (fetchedUser.getPassword().equals(password)) {

                // username & password are the same
                return true;

            }

        }

        return false;

    }

    @Override
    public void close() {

        try {

            // double clear
            this.clearTemporaryStorage();

            this.userDB.close();
            this.docDB.close();
            this.contentDigestDB.close();
            this.channelDB.close();
            this.urlFrontierDB.close();
            this.stormStateDB.close();
            this.docCounterDB.close();
            this.docIDtoDocDB.close();
            this.crawlerCatalog.close();
            this.env.close();

        } catch (DatabaseException de) {

            // TODO: update
            logger.debug("(CrawlerDB) Get DatabaseException when closing the environment!");

        }

    }

    // MS2 Added
    /**
     * Create a channel.
     * true: successfully created;
     * false: fail to create (mainly due to duplicate channel name).
     */
    @Override
    public boolean createChannel(String channelName, String username, String pattern) {

        // check channel exists or not
        if (this.channelMap.containsKey(channelName)) {

            return false;

        }

        // get date for createdDate
        String current = formatDateToString(getCurrentDate());

        // TODO: need to check from existing users?

        // init new channel
        Channel channel = new Channel(channelName, username, current, pattern);

        // add
        this.channelMap.put(channelName, channel);

        logger.debug("(Thread " + Thread.currentThread().getId()
                + " - CrawlerDB) Channel added: " + channel.toString());

        return true;

    }

    /**
     * Add a document URL into a specific channel
     */
    @Override
    public void addURLToChannel(String channelName, String url) {

        // check before add
        if (this.channelMap.containsKey(channelName)) {

            // Get value of this channel
            Channel channel = (Channel) this.channelMap.get(channelName);

            // add url
            channel.docURLs.add(url);

            // Store it back (Rewrite)
            this.channelMap.put(channelName, channel);

            logger.debug("(Thread " + Thread.currentThread().getId()
                    + " - CrawlerDB) Add doc (url): " + url + " to channel: " + channelName);

        }

    }

    /**
     * (Added for Channel Show)
     * Fetch the specified channel from the database
     */
    @Override
    public Channel getChannel(String channelName) {

        if (this.channelMap.containsKey(channelName)) {

            Channel channel = (Channel) this.channelMap.get(channelName);

            return channel;

        }

        return null;

    }

    @Override
    public ArrayList<String> getAllChannels() {

        ArrayList<String> channelNames = new ArrayList<String>();

        for (Object key : this.channelMap.keySet()) {

            channelNames.add((String) key);

        }

        return channelNames;

    }

    public HashMap<String, ArrayList<String>> getXPathToChannelMap() {

        // iterate through the channel db
        HashMap<String, ArrayList<String>> table = new HashMap<String, ArrayList<String>>();

        for (Object channelName : this.channelMap.keySet()) {

            String name = (String) channelName;

            Channel channel = (Channel) this.channelMap.get(name);

            if (!table.containsKey(channel.getPattern())) {

                table.put(channel.getPattern(), new ArrayList<String>());

            }

            table.get(channel.getPattern()).add(name);

        }

        return table;

    }

    @Override
    public Document getDocumentEntity(String url) {

        if (this.docMap.containsKey(url)) {

            Document retrievedDoc = (Document) this.docMap.get(url);

            return retrievedDoc;

        }

        return null;

    }

    /**
     * Retrive (poll) an item from frontier
     * 
     */
    @Override
    public ArrayList<URLInfo> getNextURLs() {

        ArrayList<URLInfo> nextURLs = new ArrayList<URLInfo>();

        for (Object info : this.urlFrontier.values()) {

            URLInfo urlInfo = (URLInfo) info;

            nextURLs.add(urlInfo);

        }

        this.urlFrontier.clear();

        return nextURLs;

    }

    /**
     * Append an item to frontier
     */
    @Override
    public void addURLToFrontier(URLInfo urlInfo) {

        this.urlFrontier.put("" + nextFrontierID.getAndIncrement(), urlInfo);

    }

    /**
     * Get current queue size
     */
    @Override
    public int getFrontierSize() {

        return this.urlFrontier.size();

    }

    /**
     * Update the working state of an id
     */
    @Override
    public void updateStormState(String id, Boolean working) {

        // update the state by directly overwriting
        this.stormState.put(id, working);

    }

    /**
     * Check whether spouts & bolts are stopped or not
     */
    @Override
    public boolean isStormWorking() {

        // check all the key-value pairs in stormState
        for (Object state : this.stormState.values()) {

            boolean currState = (boolean) state;

            if (currState) {

                // if anyone of them is working
                return true;

            }

        }

        return false;

    }

    /**
     * Increase the document count of each run
     */
    @Override
    public void incDocCount(String masterID) {

        int curr = 0;

        if (this.docCounter.containsKey(masterID)) {

            curr = (int) this.docCounter.get(masterID);

        }

        // add one
        this.docCounter.put(masterID, curr + 1);

        logger.info("(Thread " + Thread.currentThread().getId()
                + " - CrawlerDB) Crawler ID: " + masterID + " already fetched " + (curr + 1) + " (n) docs!");

    }

    /**
     * To know the n files fetched in this run
     * 
     * @param masterID
     */
    @Override
    public int getDocCount(String masterID) {

        return (int) this.docCounter.getOrDefault(masterID, 0);

    }

    // private void export(String filename, ArrayList<String> urls) {

    // File file = new File(filename);
    // String parent = file.getParent();

    // if (!Files.exists(Paths.get(parent))) {

    // try {

    // Files.createDirectories(Paths.get(parent));

    // } catch (IOException e) {

    // e.printStackTrace();

    // }

    // }

    // try {

    // BufferedWriter writer = new BufferedWriter(new FileWriter(filename));

    // for (String url : urls) {

    // writer.write(url + "\r\n");

    // }

    // writer.close();

    // } catch (IOException e) {

    // e.printStackTrace();
    // }

    // }

    // @Override
    // public void exportDocIDs(ArrayList<String> targetDirectories, int
    // rowsPerFile) {

    // ArrayList<String> urls = new ArrayList<String>();

    // int i = 0;

    // for (Object url : this.docMap.keySet()) {

    // String surl = (String) url;

    // urls.add(surl);

    // if (urls.size() == rowsPerFile) {

    // int target = (i % targetDirectories.size());
    // int round = i / targetDirectories.size();
    // String fn = targetDirectories.get(target) + "/" + "docURLs" + round + ".txt."
    // + target;

    // export(fn, urls);

    // urls.clear();

    // i++;

    // }

    // }

    // }

    // @Override
    // public ArrayList<Document> exportDocuments(int count) {

    // ArrayList<Document> docs = new ArrayList<Document>();

    // int nExport = 0;

    // for (Object obj : this.docMap.values()) {

    // Document doc = (Document) obj;

    // docs.add(doc);
    // nExport++;

    // if (nExport == count) {

    // break;

    // }

    // }

    // return docs;

    // }

    @Override
    public String getContent(String url) {

        if (this.docMap.containsKey(url)) {

            Document doc = (Document) this.docMap.get(url);

            return doc.getContent();

        }

        return null;

    }

    @Override
    public String getURLByDocID(int docID) {

        if (this.docIDtoDoc.containsKey(docID)) {

            Document doc = (Document) this.docIDtoDoc.get(docID);

            String url = doc.getURL();

            return url;
        }

        return null;

    }

    @Override
    public ArrayList<Integer> exportDocuments(int start, int end) {

        ArrayList<Integer> docIDs = new ArrayList<Integer>();

        for (Object obj : this.docIDtoDoc.keySet()) {

            int key = (Integer) obj;

            if (key < start) {

                continue;

            }

            if (key >= end) {

                break;

            }

            docIDs.add(key);

        }

        return docIDs;

    }

    @Override
    public Document getDocumentByID(int docID) {

        if (this.docIDtoDoc.containsKey(docID)) {

            return (Document) this.docIDtoDoc.get(docID);

        }

        return null;

    }

    /**
     * For sync url : id
     */

    /**
     * (Added for temporary db)
     * This includes frontier, content digest
     */
    @Override
    public void clearTemporaryStorage() {

        this.urlFrontier.clear();

        this.contentDigestMap.clear();

        this.stormState.clear();

        this.docCounter.clear();

    }

}
