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

public class HitStorage {

    final static Logger logger = LogManager.getLogger(HitStorage.class);

    String dbDirectory;

    // The field for the Crawler Database Environment
    private Environment env;
    private DatabaseConfig dbConfig;

    // Catalog
    private static final String classCatalog = "class_catalog";
    private StoredClassCatalog storedClassCatalog;

    /**
     * Overview of this HitStorage.
     * Note: This is a main location of all the indexed data,
     * including lexicon, Document-Words (DocWords) info, WordHits.
     * 
     * 
     * For what index server truly stores (cares):
     * 1. ShortShards DBs & ShortShards Maps: short lexicon's doc hits
     * 2. PlainShards DBs & PlainShards Maps: plain lexicon's doc hits
     * 
     */

    // hits
    // each for doc's hits
    // key (docID, wordID, tag, pos),
    // value [docID, wordID, word, tag, pos, cap, context]

    private Database shortHitsDB;
    private StoredSortedMap shortHitsMap;

    private Database plainHitsDB;
    private StoredSortedMap plainHitsMap;

    public HitStorage(String directory) throws DatabaseException {

        // Incoming local directory of a worker (e.g. storage/node1)
        this.dbDirectory = directory;

        try {

            // initiate the environment
            logger.debug("(HitStorage) Opening Env in " + this.dbDirectory);

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

            shortHitsDB = env.openDatabase(null, "short_hits", this.dbConfig);
            shortHitsMap = new StoredSortedMap(shortHitsDB,
                    new SerialBinding(this.storedClassCatalog, String.class),
                    new SerialBinding(this.storedClassCatalog, ArrayList.class),
                    true);
            plainHitsDB = env.openDatabase(null, "plain_hits", this.dbConfig);
            plainHitsMap = new StoredSortedMap(plainHitsDB,
                    new SerialBinding(this.storedClassCatalog, String.class),
                    new SerialBinding(this.storedClassCatalog, ArrayList.class),
                    true);

            logger.debug("(HitStorage) current short hits: " + this.shortHitsMap.size());
            logger.debug("(HitStorage) current plain hits: " + this.plainHitsMap.size());

        } catch (DatabaseException e) {

            throw e;

        }

    }

    /**
     * HitStorage is basically sorted by the key word,docID
     * 
     * @param word
     * @param docID
     * @return
     */
    public String getKey(String word, int docID) {
        return "" + word + "," + docID;
    }

    /**
     * Update the word hits of a given word in a document
     * 
     * @param newHits        : [List<WordHit>], basically (docID, word, tag, pos,
     *                       cap,
     *                       context)
     * @param collectionType : [String], either short or plain
     */
    public void addWordDocHits(String word, int docID, ArrayList<WordHit> newHits, String collectionType) {

        StoredSortedMap targetMap = (collectionType.equals("short")) ? this.shortHitsMap : this.plainHitsMap;

        // key : word,docID
        String key = getKey(word, docID);

        if (!targetMap.containsKey(key)) {
            targetMap.put(key, new ArrayList<WordHit>());
        }

        ArrayList<WordHit> previousHits = (ArrayList<WordHit>) targetMap.get(key);

        previousHits.addAll(newHits);

        // store it back
        targetMap.put(key, previousHits);

    }

    /**
     * 
     * @param word      : [String]
     * @param docID     : [int]
     * @param fromShort : [boolean] from short hits or plain hits
     *                  true for short hits
     * @return
     */
    public ArrayList<WordHit> getWordHitList(String word, int docID, String collectionType) {

        String key = getKey(word, docID);

        StoredSortedMap targetMap = (collectionType.equals("short")) ? this.shortHitsMap : this.plainHitsMap;

        if (targetMap.containsKey(key)) {

            ArrayList<WordHit> hits = (ArrayList<WordHit>) targetMap.get(key);

            return hits;

        }

        // return empty is not found
        logger.debug("(HitStorage) No found matches for key: " + key + " in collection :" + collectionType);

        return new ArrayList<WordHit>();

    }

    /**
     * Show the statistics of the word hit
     * 
     * @param verbose
     */
    public void showLocalHitStatistics(boolean verbose) {

        logger.info("Current short hits : " + this.shortHitsMap.size() + " (n)");

        int nToShow = 5;
        if (verbose) {

            int cnt = 0;
            for (Object obj : this.shortHitsMap.keySet()) {

                String key = (String) obj;

                ArrayList<WordHit> hits = (ArrayList<WordHit>) this.shortHitsMap.get(key);

                logger.info("--- short --- key : " + key + " => array: " + hits.toString());

                cnt++;

                if (cnt == nToShow) {

                    break;

                }

            }

        }

        logger.info("Current plain hits : " + this.plainHitsMap.size() + " (n)");

        if (verbose) {

            int cnt = 0;
            for (Object obj : this.plainHitsMap.keySet()) {

                String key = (String) obj;

                ArrayList<WordHit> hits = (ArrayList<WordHit>) this.plainHitsMap.get(key);

                logger.info("--- plain --- key : " + key + " => array: " + hits.toString());

                cnt++;

                if (cnt == nToShow) {

                    break;

                }

            }

        }

    }

    // TODO: safe close all
    public void close() {

        try {

            this.shortHitsDB.close();
            this.plainHitsDB.close();

            this.storedClassCatalog.close();

            this.env.close();

        } catch (DatabaseException de) {

            // TODO: update
            logger.debug("(LocalHit) Get DatabaseException when closing the indexer db environment!");

        }

    }

    public static void main(String args[]) {

        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn", Level.DEBUG);

        if (args.length < 2) {
            System.out.println("Usage: [hit storage directory] [verbose]");
            System.exit(1);
        }

        HitStorage hitDB = StorageFactory.getHitDatabase(args[0]);

        hitDB.showLocalHitStatistics(Boolean.parseBoolean(args[1]));

        hitDB.close();

    }

}
