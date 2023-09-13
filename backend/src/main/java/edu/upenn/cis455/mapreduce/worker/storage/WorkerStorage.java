package edu.upenn.cis455.mapreduce.worker.storage;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.collections.StoredSortedMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WorkerStorage {

    final static Logger logger = LogManager.getLogger(WorkerStorage.class);

    String dbDirectory;

    // The field for the Crawler Database Environment
    private Environment env;
    private DatabaseConfig dbConfig;

    // Catalog
    private static final String classCatalog = "class_catalog";
    private StoredClassCatalog storedClassCatalog;

    // database collection
    // executorID: Database / StoredSortedMap
    private HashMap<String, Database> databases = new HashMap<String, Database>();
    private HashMap<String, StoredSortedMap> sortedMaps = new HashMap<String, StoredSortedMap>();

    // private AtomicInteger mapIDs = new AtomicInteger(0);

    public WorkerStorage(String directory) throws DatabaseException {

        // Incoming local directory of a worker (e.g. storage/node1)
        this.dbDirectory = directory;

        try {

            // initiate the environment
            logger.debug("(WorkerStorage) Opening Env in " + this.dbDirectory);

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

        } catch (DatabaseException e) {

            throw e;

        }

    }

    /**
     * Add a database / table to the worker's DB environment.
     * 
     * @param id         : [String], executorID
     * @param keyClass   : [Class] (e.g. String.class)
     * @param valueClass : [Class] (e.g. String.class)
     */
    public void registerTable(String id, Class keyClass, Class valueClass) {

        // init database
        Database newDB = env.openDatabase(null, id, this.dbConfig);

        this.databases.put(id, newDB);

        // init map
        EntryBinding keyBinding = new SerialBinding(this.storedClassCatalog, keyClass);

        EntryBinding valueBinding = new SerialBinding(this.storedClassCatalog, valueClass);

        StoredSortedMap newTable = new StoredSortedMap(newDB, keyBinding, valueBinding, true);

        this.sortedMaps.put(id, newTable);

    }

    // TODO: Add interfaces for adding keys & values

    /**
     * Add a key-value pair to the table.
     * If the key exists, value is added to the collection of the key (as a list).
     * 
     * @param id    : [String], executorID
     * @param key   : [String]
     * @param value : [String], can be null
     */
    public void addKeyValuePair(String id, String key, String value) {

        // check table exist or not
        if (!this.sortedMaps.containsKey(id)) {

            logger.debug("(WorkerStorage) no such table (" + id + ") exist!");

            return;

        }

        // get table
        StoredSortedMap table = this.sortedMaps.get(id);

        // check key exists or not
        if (!table.containsKey(key)) {

            // create new collection
            table.put(key, new ArrayList<String>());

        }

        // logger.debug("(WorkerStorage) Id " + id + " : Key " + key);

        // add new value
        if (value != null) {

            ArrayList<String> currValues = (ArrayList<String>) table.get(key);
            currValues.add(value);

            // logger.debug("(WorkerStorage) Id " + id + " : Add value " + value);
            // logger.debug("(WorkerStorage) Id " + id + " : key " + key + ", All => " +
            // currValues.toString());

            // store back
            table.put(key, currValues);

        }

    }

    /**
     * (deprecated)For ReduceBolt to record a single word hit efficiently.
     * 
     * @param id    : [String] executorID
     * @param key   : [String], word-|-docID-|-tag-|-pos
     * @param value : [String[]], (context, docID, url, title, tag, pos, cap)
     */
    public void addIntermediateWordHit(String id, String key, String[] value) {

        // check table exist or not
        if (!this.sortedMaps.containsKey(id)) {

            logger.debug("(WorkerStorage) no such table (" + id + ") exist!");

            return;

        }

        // get table
        StoredSortedMap table = this.sortedMaps.get(id);

        // verify : as key is unique, should not have this key
        if (table.containsKey(key)) {

            logger.debug("(WorkerStorage) should not have duplicate : " + key);

        }

        table.put(key, value);

    }

    /**
     * For ReduceBolt to record a single word hit efficiently.
     * 
     * @param id    : [String] executorID
     * @param key   : [String[]], (word, docID, tag, pos)
     * @param value : [String[]], (context, docID, url, title, tag, pos, cap)
     */
    public void addIntermediateWordHit(String id, String[] key, String[] value) {

        // check table exist or not
        if (!this.sortedMaps.containsKey(id)) {

            logger.debug("(WorkerStorage) no such table (" + id + ") exist!");

            return;

        }

        // get table
        StoredSortedMap table = this.sortedMaps.get(id);

        // verify : as key is unique, should not have this key
        if (table.containsKey(key)) {

            logger.debug("(WorkerStorage) should not have duplicate : " + key);

        }

        table.put(key, value);

    }

    /**
     * Directly get the worker's whole map
     * 
     * @param id
     * @return
     */
    public StoredSortedMap retrieveWorkerMap(String id) {

        // check table exist or not
        if (!this.sortedMaps.containsKey(id)) {

            logger.debug("(WorkerStorage) no such table (" + id + ") exist!");

            return null;

        }

        return this.sortedMaps.get(id);

    }

    /**
     * 
     * @param id        : [String]
     * @param batchSize : [int]
     * @return
     */
    public ArrayList<String[]> retrieveBatchKeys(String id, int batchSize) {

        // check table exist or not
        if (!this.sortedMaps.containsKey(id)) {

            logger.debug("(WorkerStorage) no such table (" + id + ") exist!");

            return null;

        }

        // ArrayList<String> batch = new ArrayList<String>();
        ArrayList<String[]> batch = new ArrayList<String[]>();

        int cnt = 0;

        for (Object key : this.sortedMaps.get(id).keySet()) {

            // batch.add((String) key);
            batch.add((String[]) key);

            cnt++;

            if (cnt == batchSize) {

                return batch;

            }

        }

        return batch;

    }

    public String[] getIntermediateWordHit(String id, String key[]) {

        // check table exist or not
        if (!this.sortedMaps.containsKey(id)) {

            logger.debug("(WorkerStorage) no such table (" + id + ") exist!");

            return null;

        }

        return (String[]) this.sortedMaps.get(id).get(key);

    }

    public void removeIntermediateHits(String id, ArrayList<String[]> tasksDone) {

        // check table exist or not
        if (!this.sortedMaps.containsKey(id)) {

            logger.debug("(WorkerStorage) no such table (" + id + ") exist!");

            return;

        }

        for (String[] key : tasksDone) {

            if (this.sortedMaps.get(id).containsKey(key)) {

                this.sortedMaps.get(id).remove(key);

            }

        }

    }

    public Set<String> getStoredKeys(String id) {

        // check table exist or not
        if (!this.sortedMaps.containsKey(id)) {

            logger.debug("(WorkerStorage) no such table (" + id + ") exist!");

            return null;

        }

        return this.sortedMaps.get(id).keySet();

    }

    public ArrayList<String> getValuesByKey(String id, String key) {

        // check table exist or not
        if (!this.sortedMaps.containsKey(id)) {

            logger.debug("(WorkerStorage) no such table (" + id + ") exist!");

            return null;

        }

        return (ArrayList<String>) this.sortedMaps.get(id).get(key);

    }

    public ArrayList<String[]> getWordHitsByKey(String id, String key) {

        // check table exist or not
        if (!this.sortedMaps.containsKey(id)) {

            logger.debug("(WorkerStorage) no such table (" + id + ") exist!");

            return null;

        }

        return (ArrayList<String[]>) this.sortedMaps.get(id).get(key);

    }

    public Integer getCountByKey(String id, String key) {

        // check table exist or not
        if (!this.sortedMaps.containsKey(id)) {

            logger.debug("(WorkerStorage) no such table (" + id + ") exist!");

            return null;

        }

        return (Integer) this.sortedMaps.get(id).get(key);

    }

    public Set<Map.Entry<String, Integer>> getEntrySet(String id) {

        // check table exist or not
        if (!this.sortedMaps.containsKey(id)) {

            logger.debug("(WorkerStorage) no such table (" + id + ") exist!");

            return null;

        }

        return this.sortedMaps.get(id).entrySet();

    }

    // TODO: clear database or delete database
    public void clearTable(String id) {

        StoredSortedMap table = this.sortedMaps.getOrDefault(id, null);

        if (table == null) {

            return;

        }

        // clear the table
        table.clear();

    }

    // TODO: safe close all
    public void close() {

        try {

            // iterate and close all the existing databases
            for (Map.Entry<String, Database> entry : this.databases.entrySet()) {

                Database db = entry.getValue();

                db.close();

            }

            this.storedClassCatalog.close();

            this.env.close();

        } catch (DatabaseException de) {

            // TODO: update
            logger.debug("(WorkerStorage) Get DatabaseException when closing the worker db environment!");

        }

    }

}
