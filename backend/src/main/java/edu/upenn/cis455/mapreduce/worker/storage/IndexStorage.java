package edu.upenn.cis455.mapreduce.worker.storage;

import org.apache.logging.log4j.Level;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.collections.StoredSortedMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis455.mapreduce.worker.storage.entities.DocInfo;
import edu.upenn.cis455.mapreduce.worker.storage.entities.DocWord;
import edu.upenn.cis455.mapreduce.worker.storage.entities.WordHit;
import edu.upenn.cis455.mapreduce.worker.storage.entities.Word;

public class IndexStorage {

    final static Logger logger = LogManager.getLogger(IndexStorage.class);

    String dbDirectory;

    // The field for the Crawler Database Environment
    private Environment env;
    private DatabaseConfig dbConfig;

    // Catalog
    private static final String classCatalog = "class_catalog";
    private StoredClassCatalog storedClassCatalog;

    /**
     * Overview of this IndexStorage.
     * Note: This is a main location of all the indexed data,
     * including lexicon, Document-Words (DocWords) info, WordHits.
     * 
     * For local check purpose:
     * 1. seenDoc & seenDocMap: the docIDs of the parsed documents
     * 2. wordIndex & wordIndexMap: word to index (wordID)
     * 3. indexWord & indexWordMap: index (wordID) to word
     * 
     * For uploading (synchronizing) with remote DBs (the parsed results):
     * 4. uploadShortDocDB & uploadShortDocMap:
     * ---- the DocWords info from the docIDs to be uploaded
     * 5. uploadPlainDocDB & uploadPlainDocMap:
     * ---- the DocWords info from the docIDs to beuploaded
     * 6. uploadShortLexiconDB & uploadShortLexiconMap:
     * ---- the info of whether a word is uploaded before or not
     * 7. uploadPlainLexiconDB & uploadPlainLexiconMap:
     * ---- the info of whether a word is uploaded before or not
     * 
     * For what index server truly stores (cares):
     * 8. ShortLexiconDB & ShortLexiconMap: short lexicon
     * 9. ShortDocDB & ShortDocMap: short DocWords
     * 10. PlainLexiconDB & PlainLexiconMap: plain lexicon
     * 11. PlainDocDB & PlainDocMap: plain DocWords
     * 12. DocInfoDB & DocInfoMap: A copy of docoument related information
     * 
     */

    // index tables
    private String docDBName = "seen_docs";
    private String w2iDBName = "word_id";
    private String i2wDBName = "id_word";
    private Database seenDoc;
    private Database wordIndex;
    private Database indexWord;

    // for retrieval & check
    private StoredSortedMap seenDocMap;
    private StoredSortedMap wordIndexMap;
    private StoredSortedMap indexWordMap;

    // for synchronization with remote
    // format:
    // key (docID,wordID), value : docID
    private Database uploadShortDocDB;
    private StoredSortedMap uploadShortDocMap;

    private Database uploadPlainDocDB;
    private StoredSortedMap uploadPlainDocMap;

    // key wordID, value wordID
    private Database uploadShortLexiconDB;
    private StoredSortedMap uploadShortLexiconMap;
    private Database uploadPlainLexiconDB;
    private StoredSortedMap uploadPlainLexiconMap;

    private Database uploadDocInfoDB;
    private StoredSortedMap uploadDocInfoMap;

    // index storage
    // Format

    // Lex : key (wordID), value [word, wordID, df, idf]
    // Doc (DocWord) : key (docID, wordID) , value [tf, wtf]
    // Doc : key (docID), value : DocWords
    private Database shortDocDB;
    private Database shortLexiconDB;
    private StoredSortedMap shortDocMap;
    private StoredSortedMap shortLexiconMap;

    private Database plainDocDB;
    private Database plainLexiconDB;
    private StoredSortedMap plainDocMap;
    private StoredSortedMap plainLexiconMap;

    // docID: (docID, title, url)
    private Database docInfoDB;
    private StoredSortedMap docInfoMap;

    public AtomicInteger nextWordID;

    public IndexStorage(String directory) throws DatabaseException {

        // Incoming local directory of a worker (e.g. storage/node1)
        this.dbDirectory = directory;

        try {

            // initiate the environment
            logger.debug("(IndexStorage) Opening Env in " + this.dbDirectory);

            EnvironmentConfig envConfig = new EnvironmentConfig();

            // setTransactional to true for persistent storage
            // otherwise, the data won't be stored
            envConfig.setAllowCreate(true);
            envConfig.setTransactional(true);

            this.env = new Environment(new File(this.dbDirectory), envConfig);

            // database config
            dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(true);

            // catalog
            Database catalogDB = env.openDatabase(null, classCatalog, dbConfig);
            this.storedClassCatalog = new StoredClassCatalog(catalogDB);

            seenDoc = env.openDatabase(null, docDBName, this.dbConfig);
            seenDocMap = new StoredSortedMap(seenDoc,
                    new SerialBinding(this.storedClassCatalog, Integer.class),
                    new SerialBinding(this.storedClassCatalog, Integer.class),
                    true);

            wordIndex = env.openDatabase(null, w2iDBName, this.dbConfig);
            wordIndexMap = new StoredSortedMap(wordIndex,
                    new SerialBinding(this.storedClassCatalog, String.class),
                    new SerialBinding(this.storedClassCatalog, Integer.class),
                    true);

            indexWord = env.openDatabase(null, i2wDBName, this.dbConfig);
            indexWordMap = new StoredSortedMap(indexWord,
                    new SerialBinding(this.storedClassCatalog, Integer.class),
                    new SerialBinding(this.storedClassCatalog, String.class),
                    true);

            int totalWords = this.indexWordMap.size();
            nextWordID = new AtomicInteger(totalWords);

            // tables for synchronization with remote DBs
            this.initRemoteSyncDB();

            this.initLocalIndexStorageDB();

            logger.debug("(IndexStorage) Already index : " + this.seenDocMap.size() + " (n) docs!");
            logger.debug("(IndexStorage) Next word index: " + nextWordID);

        } catch (DatabaseException e) {

            throw e;

        }

    }

    private void initRemoteSyncDB() {

        // key : (docID, word), value : docID
        uploadShortDocDB = env.openDatabase(null, "upload_short_doc", this.dbConfig);
        uploadShortDocMap = new StoredSortedMap(uploadShortDocDB,
                new SerialBinding(this.storedClassCatalog, String.class),
                new SerialBinding(this.storedClassCatalog, DocWord.class),
                true);

        uploadPlainDocDB = env.openDatabase(null, "upload_plain_doc", this.dbConfig);
        uploadPlainDocMap = new StoredSortedMap(uploadPlainDocDB,
                new SerialBinding(this.storedClassCatalog, String.class),
                new SerialBinding(this.storedClassCatalog, DocWord.class),
                true);

        uploadShortLexiconDB = env.openDatabase(null, "upload_short_lexicon",
                this.dbConfig);
        uploadShortLexiconMap = new StoredSortedMap(uploadShortLexiconDB,
                new SerialBinding(this.storedClassCatalog, Integer.class),
                new SerialBinding(this.storedClassCatalog, Integer.class),
                true);

        uploadPlainLexiconDB = env.openDatabase(null, "upload_plain_lexicon",
                this.dbConfig);
        uploadPlainLexiconMap = new StoredSortedMap(uploadPlainLexiconDB,
                new SerialBinding(this.storedClassCatalog, Integer.class),
                new SerialBinding(this.storedClassCatalog, Integer.class),
                true);

        uploadDocInfoDB = env.openDatabase(null, "upload_doc_info", this.dbConfig);
        uploadDocInfoMap = new StoredSortedMap(uploadDocInfoDB,
                new SerialBinding(this.storedClassCatalog, Integer.class),
                new SerialBinding(this.storedClassCatalog, Integer.class),
                true);

    }

    private void initLocalIndexStorageDB() {

        shortDocDB = env.openDatabase(null, "short_doc", this.dbConfig);
        shortDocMap = new StoredSortedMap(shortDocDB,
                new SerialBinding(this.storedClassCatalog, String.class),
                new SerialBinding(this.storedClassCatalog, DocWord.class),
                true);

        logger.debug("(IndexStorage) Current short doc: " + shortDocMap.size());

        plainDocDB = env.openDatabase(null, "plain_doc", this.dbConfig);
        plainDocMap = new StoredSortedMap(plainDocDB,
                new SerialBinding(this.storedClassCatalog, String.class),
                new SerialBinding(this.storedClassCatalog, DocWord.class),
                true);

        logger.debug("(IndexStorage) Current plain doc: " + plainDocMap.size());

        shortLexiconDB = env.openDatabase(null, "short_lexicon", this.dbConfig);
        shortLexiconMap = new StoredSortedMap(shortLexiconDB,
                new SerialBinding(this.storedClassCatalog, Integer.class),
                new SerialBinding(this.storedClassCatalog, Word.class),
                true);

        logger.debug("(IndexStorage) Current short lexicon: " + shortLexiconMap.size());

        plainLexiconDB = env.openDatabase(null, "plain_lexicon", this.dbConfig);
        plainLexiconMap = new StoredSortedMap(plainLexiconDB,
                new SerialBinding(this.storedClassCatalog, Integer.class),
                new SerialBinding(this.storedClassCatalog, Word.class),
                true);

        logger.debug("(IndexStorage) Current plain lexicon: " + plainLexiconMap.size());

        docInfoDB = env.openDatabase(null, "doc_info", this.dbConfig);
        docInfoMap = new StoredSortedMap(docInfoDB,
                new SerialBinding(this.storedClassCatalog, Integer.class),
                new SerialBinding(this.storedClassCatalog, DocInfo.class),
                true);

    }

    /**
     * Retrieve or add new document ID
     * 
     * @param word
     * @return
     */
    public int getWordID(String word) {

        if (this.wordIndexMap.containsKey(word)) {

            // already indexed the word
            int wordID = (int) this.wordIndexMap.get(word);

            return wordID;

        }

        // not indexed
        int wordID = nextWordID.getAndIncrement();

        // store this mapping
        this.wordIndexMap.put(word, wordID);
        this.indexWordMap.put(wordID, word);

        return wordID;

    }

    public boolean hasWord(String word) {

        if (this.wordIndexMap.containsKey(word)) {

            return true;

        }

        return false;

    }

    /**
     * Check the document seen or not by ID.
     * This is used for task schedule. Should not use by others.
     * 
     * @param docID
     * @return
     */
    public boolean hasSeenDocument(int docID) {

        if (this.seenDocMap.containsKey(docID)) {

            // logger.debug("Has seen doc id : " + docID);

            return true;

        }

        return false;

    }

    /**
     * Used by the final step in IndexUpdateBolt
     * 
     * @param docID
     */
    public void addDocumentID(int docID) {

        // logger.debug("(IndexStorage) Add document ID: " + docID);

        this.seenDocMap.put(docID, docID);

    }

    /**
     * unique by docID,word
     * 
     * @param docID
     * @param word
     * @param updateOrInsert: [int], 0: insert, 1: update,
     * @return
     */
    public String getUploadDocWordKey(int docID, String word, int updateOrInsert) {

        return "" + updateOrInsert + "|" + docID + "|" + word;
    }

    /**
     * Add DocWord to ShortDocMap's DocWords.
     * Note: each job should only call each (docID, word) pair once.
     * Each round, the number of words in each doc should be aggregated at first.
     * 
     * @param docID
     * @param word
     * @param tf
     * @param ntf            deprecated, calculate by remote server
     * @param collectionType : [String], either short or plain
     */
    public void updateDocWord(int docID, String word, int tf, String collectionType) {

        StoredSortedMap targetMap = (collectionType.equals("short")) ? this.shortDocMap : this.plainDocMap;
        StoredSortedMap targetUploadMap = (collectionType.equals("short")) ? this.uploadShortDocMap
                : this.uploadPlainDocMap;

        // key: docID,wordID => docID, word
        String key = "" + docID + "," + word;

        if (!targetMap.containsKey(key)) {

            // no such docID,word pair
            DocWord docWord = new DocWord(docID, word, tf);
            targetMap.put(key, docWord);

            // should insert
            // targetUploadMap.put(getUploadDocWordKey(docID, word, 0), docWord);

        } else {

            // for the first time, following message should not show up
            // for the following time, it might show up (because of <a> tag)
            // logger.debug("Encounter key: " + key + " again! Should not !!!");

            // update
            DocWord docWord = (DocWord) targetMap.get(key);
            docWord.addWordCount(tf);

            // store
            targetMap.put(key, docWord);

            // should update
            // targetUploadMap.put(getUploadDocWordKey(docID, word, 1), docWord);

        }

    }

    public StoredSortedMap getDocWordMap(String collectionType) {

        StoredSortedMap targetMap = (collectionType.equals("short")) ? this.shortDocMap : this.plainDocMap;

        return targetMap;

    }

    public int getLexiconSize(String collectionType) {

        if (collectionType.equals("short")) {

            return this.shortLexiconMap.size();

        } else if (collectionType.equals("plain")) {

            return this.plainLexiconMap.size();
        }

        return 0;

    }

    /**
     * Update lexicon
     * 
     * @param wordID
     * @param word
     * @param df
     * @param idf
     * @param collectionType : [String], either short or plain
     */
    public void updateLexicon(int wordID, String word, int df, int localPos, String collectionType) {

        StoredSortedMap targetMap = (collectionType.equals("short")) ? this.shortLexiconMap : this.plainLexiconMap;
        StoredSortedMap targetUploadMap = (collectionType.equals("short")) ? this.uploadShortLexiconMap
                : this.uploadPlainLexiconMap;

        if (targetMap.containsKey(wordID)) {

            // fetch & add up
            Word wd = (Word) targetMap.get(wordID);

            wd.addDf(df);

            targetMap.put(wordID, wd);

        } else {

            // unseen
            targetMap.put(wordID, new Word(wordID, word, df, localPos));

            // to be added
            // targetUploadMap.put(wordID, wordID);

        }

    }

    public StoredSortedMap getLexiconMap(String collectionType) {

        StoredSortedMap targetMap = (collectionType.equals("short")) ? this.shortLexiconMap : this.plainLexiconMap;

        return targetMap;

    }

    /**
     * Add new document info
     * 
     * @param docID
     * @param url
     * @param title
     */
    public void addDocInfo(int docID, String url, String title, String context) {

        if (!this.docInfoMap.containsKey(docID)) {

            this.docInfoMap.put(docID, new DocInfo(docID, url, title, context));

            // this.uploadDocInfoMap.put(docID, docID);

        }

    }

    /**
     * Retrieve doc info map directly
     * 
     * @return
     */
    public StoredSortedMap getDocInfoMap() {

        return this.docInfoMap;

    }

    public DocInfo getDocInfo(int docID) {

        if (this.docInfoMap.containsKey(docID)) {

            return (DocInfo) this.docInfoMap.get(docID);

        }

        return null;

    }

    /**
     * The following section is for synchronization
     * ShortDocUpload's key docID|wordID|0(update) or 1(insert)
     */
    public HashMap<String, DocWord> getDocWordUploadTasks(int count, String collectionType) {

        StoredSortedMap targetUploadMap = (collectionType.equals("short")) ? this.uploadShortDocMap
                : this.uploadPlainDocMap;

        HashMap<String, DocWord> tasks = new HashMap<String, DocWord>();

        int i = 0;

        for (Object obj : targetUploadMap.keySet()) {

            String key = (String) obj;

            tasks.put(key, (DocWord) targetUploadMap.get(key));

            i++;

            if (i == count) {

                break;

            }

        }

        return tasks;

    }

    /**
     * Remove tasks done in the upload db
     * 
     * @param tasksDone
     * @param collectionType
     */
    public void removeDocWordUploadTasks(Set<String> tasksDone, String collectionType) {

        StoredSortedMap targetUploadMap = (collectionType.equals("short")) ? this.uploadShortDocMap
                : this.uploadPlainDocMap;

        for (String key : tasksDone) {

            if (targetUploadMap.containsKey(key)) {

                targetUploadMap.remove(key);

            }

        }

    }

    /**
     * Get the upload tasks for lexicon
     * 
     * @param count
     * @param collectionType
     * @return
     */
    public HashMap<Integer, Word> getLexiconUploadTasks(int count, String collectionType) {

        StoredSortedMap targetMap = (collectionType.equals("short")) ? this.shortLexiconMap
                : this.plainLexiconMap;

        StoredSortedMap targetUploadMap = (collectionType.equals("short")) ? this.uploadShortLexiconMap
                : this.uploadPlainLexiconMap;

        HashMap<Integer, Word> tasks = new HashMap<Integer, Word>();

        int i = 0;

        for (Object obj : targetUploadMap.keySet()) {

            int wordID = (Integer) obj;

            Word wd = (Word) targetMap.get(wordID);

            tasks.put(wordID, wd);

            i++;

            if (i == count) {

                break;

            }

        }

        return tasks;

    }

    /**
     * Remove the tasks done for lexicon
     * 
     * @param tasksDone
     */
    public void removeLexiconUploadTasks(Set<Integer> tasksDone, String collectionType) {

        StoredSortedMap targetUploadMap = (collectionType.equals("short")) ? this.uploadShortLexiconMap
                : this.uploadPlainLexiconMap;

        for (int key : tasksDone) {

            if (targetUploadMap.containsKey(key)) {

                targetUploadMap.remove(key);

            }

        }

    }

    /**
     * Get the upload tasks for DocInfo
     * 
     * @param count
     * @return
     */
    public HashMap<Integer, DocInfo> getDocInfoUploadTasks(int count) {

        HashMap<Integer, DocInfo> tasks = new HashMap<Integer, DocInfo>();

        int i = 0;

        for (Object obj : this.uploadDocInfoMap.keySet()) {

            int key = (Integer) obj;

            DocInfo info = (DocInfo) this.docInfoMap.get(key);

            tasks.put(key, info);

            i++;

            if (i == count) {

                break;

            }

        }

        return tasks;

    }

    /**
     * Remove the tasks done in DocInfo
     * 
     * @param tasksDone
     */
    public void removeDocInfoUploadTasks(Set<Integer> tasksDone) {

        for (int key : tasksDone) {

            if (this.uploadDocInfoMap.containsKey(key)) {

                this.uploadDocInfoMap.remove(key);

            }

        }

    }

    /**
     * Get word by wordID
     * 
     * @param wordID
     * @param collectionType : [String]
     * @return
     */
    public Word getLexicon(int wordID, String collectionType) {

        StoredSortedMap targetMap = (collectionType.equals("short")) ? this.shortLexiconMap
                : this.plainLexiconMap;

        if (targetMap.containsKey(wordID)) {

            return (Word) targetMap.get(wordID);

        }

        return null;

    }

    /**
     * This is used to clear all the entries in upload* maps
     */
    public void clearAllUpdates() {

        this.uploadDocInfoMap.clear();
        this.uploadShortLexiconMap.clear();
        this.uploadPlainLexiconMap.clear();
        this.uploadPlainDocMap.clear();
        this.uploadShortDocMap.clear();

    }

    public void showIndexStorageStatistics(boolean verbose) {

        logger.debug("---- DocInfo : " + this.docInfoMap.size() + " (n) ----");
        logger.debug("-------- un-uploaded: " + this.uploadDocInfoMap.size() + " (n) ----");

        int maxShow = 20;

        if (verbose) {
            for (Object obj : this.docInfoMap.values()) {

                DocInfo info = (DocInfo) obj;

                logger.debug("DocInfo: " + info.toString());

            }
        }

        // short Lexicon
        logger.debug("---- Short Lexicon : " + this.shortLexiconMap.size() + " (n) ----");
        logger.debug("-------- un-uploaded: " + this.uploadShortLexiconMap.size() + " (n)");

        if (verbose) {

            int cnt = 0;

            for (Object obj : this.shortLexiconMap.values()) {

                Word wd = (Word) obj;

                logger.debug("Short Lexicon: " + wd.toString());

                cnt++;

                if (cnt == maxShow) {

                    break;

                }

            }
        }

        logger.debug("---- Plain Lexicon : " + this.plainLexiconMap.size() + " (n) ----");
        logger.debug("-------- un-uploaded: " + this.uploadPlainLexiconMap.size() + " (n)");

        // short DocWords
        logger.debug("---- Short DocWords : " + this.shortDocMap.size() + " (n) ----");
        logger.debug("-------- un-uploaded: " + this.uploadShortDocMap.size() + " (n)");

        if (verbose) {

            int cnt = 0;

            for (Object obj : this.shortDocMap.values()) {

                DocWord docWord = (DocWord) obj;

                logger.debug("Short DocWords: " + docWord.toString());

                cnt++;

                if (cnt == maxShow) {

                    break;

                }

            }
        }

        logger.debug("---- Plain DocWords : " + this.plainDocMap.size() + " (n) ----");
        logger.debug("-------- un-uploaded: " + this.uploadPlainDocMap.size() + " (n)");

    }

    // TODO: safe close all
    public void close() {

        try {

            this.seenDoc.close();
            this.wordIndex.close();
            this.indexWord.close();

            // sync
            this.uploadShortDocDB.close();
            this.uploadPlainDocDB.close();
            this.uploadShortLexiconDB.close();
            this.uploadPlainLexiconDB.close();

            // index data
            this.shortDocDB.close();
            this.plainDocDB.close();
            this.shortLexiconDB.close();
            this.plainLexiconDB.close();

            this.docInfoDB.close();
            this.uploadDocInfoDB.close();

            this.storedClassCatalog.close();

            this.env.close();

        } catch (DatabaseException de) {

            // TODO: update
            logger.debug("(IndexStorage) Get DatabaseException when closing the indexer db environment!");

        }

    }

    public static void main(String args[]) {

        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn", Level.DEBUG);

        if (args.length < 1) {

            System.out.println(
                    "Usage: please provide 1 argument: [index storage path]");
            return;

        }

        IndexStorage indexDB = StorageFactory.getIndexDatabase(args[0]);

        indexDB.showIndexStorageStatistics(true);

        indexDB.close();

    }

}
