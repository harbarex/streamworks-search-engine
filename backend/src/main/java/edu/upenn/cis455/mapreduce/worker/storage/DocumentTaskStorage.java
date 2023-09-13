package edu.upenn.cis455.mapreduce.worker.storage;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.collections.StoredSortedMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.cis455.storage.Document;

public class DocumentTaskStorage {

    /**
     * This class is used to assign tasks to individual workers.
     * Two main things:
     * (i) add docs / urls (related info)
     * (ii) provide doc / URL tasks to local worker
     */

    final static Logger logger = LogManager.getLogger(DocumentTaskStorage.class);

    String dbDirectory;

    // The field for the Crawler Database Environment
    private Environment env;
    private DatabaseConfig dbConfig;

    // Catalog
    private static final String classCatalog = "class_catalog";
    private StoredClassCatalog storedClassCatalog;

    // Table: docID : Document (tasks)
    private String docDBName = "documents";
    private Database docDB;
    private StoredSortedMap docMap;

    // Table (reverse mapping): URL : docID
    // This one should sync with all the mappings from Crawler
    private String urlToDocIDDBName = "url_to_doc";
    private Database urlToDocIDDB;
    private StoredSortedMap urlToDocIDMap;

    private String docIDToURLDBName = "doc_to_url";
    private Database docIDToURLDB;
    private StoredSortedMap docIDToURLMap;

    public DocumentTaskStorage(String directory) throws DatabaseException {

        // Incoming local directory of a worker (e.g. storage/node1)
        this.dbDirectory = directory;

        try {

            // initiate the environment
            logger.debug("(DocumentTaskStorage) Opening Env in " + this.dbDirectory);

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

            docDB = env.openDatabase(null, docDBName, this.dbConfig);

            // TODO: change to server side type
            docMap = new StoredSortedMap(docDB,
                    new SerialBinding(this.storedClassCatalog, Integer.class),
                    new SerialBinding(this.storedClassCatalog, Document.class),
                    true);

            // TODO: may need to update
            urlToDocIDDB = env.openDatabase(null, urlToDocIDDBName, this.dbConfig);
            urlToDocIDMap = new StoredSortedMap(urlToDocIDDB,
                    new SerialBinding(this.storedClassCatalog, String.class),
                    new SerialBinding(this.storedClassCatalog, Integer.class),
                    true);

            docIDToURLDB = env.openDatabase(null, docIDToURLDBName, this.dbConfig);
            docIDToURLMap = new StoredSortedMap(docIDToURLDB,
                    new SerialBinding(this.storedClassCatalog, Integer.class),
                    new SerialBinding(this.storedClassCatalog, String.class),
                    true);

        } catch (DatabaseException e) {

            throw e;

        }

    }

    /**
     * Sync url : docID
     */
    public void syncURLToDocID(String url, int docID) {

        this.urlToDocIDMap.put(url, docID);
        this.docIDToURLMap.put(docID, url);

    }

    public void showCurrentURLsToDocIDsSize() {

        logger.info("(DocumentTaskStorage) Current size of URLsToDocID DB: " + this.urlToDocIDMap.size());

    }

    /**
     * Get the docID based on the url
     * (-1: no such url exist)
     * 
     * @param documentID
     * @param document
     */
    public int getIDByURL(String url) {

        if (this.urlToDocIDMap.containsKey(url)) {

            return (Integer) this.urlToDocIDMap.get(url);

        }

        // logger.debug("No such url in the synced DocumentTaskStorage! URL: " + url);

        return -1;

    }

    /**
     * Add new task
     * TODO: change to api/data/Document
     * 
     * @param documentID : [int]
     * @param document   : [Document] (has id, url, content, date,
     *                   ... if there is another attribute ...)
     */
    public void addNewDocumentTask(int documentID, Document document) {

        if (!this.docMap.containsKey(documentID)) {

            // ID : Document
            this.docMap.put(documentID, document);

        }

    }

    /**
     * TODO: change to api/data/Document
     * Retrieve the document to parse.
     * 
     * @param id
     * @return
     */
    public Document getDocumentByID(int id) {

        return (Document) this.docMap.get(id);

    }

    /**
     * Retrieve the url of a document by ID
     * 
     * @param id
     * @return
     */
    public String getURLByID(int id) {

        String url = (String) this.docIDToURLMap.get(id);

        return url;

    }

    /**
     * Retrieve existing tasks to do
     * 
     * @return
     */
    public int getCurrentNumberOfTasks() {

        return this.docMap.size();

    }

    /**
     * Provde a iterator iterating through the to do doc IDs
     * 
     * @return
     */
    public ListIterator<Integer> getToDoTaskIDs() {

        ArrayList<Integer> docIDs = new ArrayList<Integer>(this.docMap.keySet());

        return docIDs.listIterator();

    }

    /**
     * Export tasks to txt file. (Useless now)
     * Format: [docID \t url \t content]
     */
    public boolean exportTasksToFile(String filename) {

        File file = new File(filename);
        String parent = file.getParent();

        if (!Files.exists(Paths.get(parent))) {

            try {

                Files.createDirectories(Paths.get(parent));

            } catch (IOException e) {

                e.printStackTrace();

            }

        }

        try {

            BufferedWriter writer = new BufferedWriter(new FileWriter(filename));

            for (Object i : this.docMap.keySet()) {

                int id = (int) i;

                Document doc = (Document) this.docMap.get(id);

                String line = "" + id + "\t" + doc.getURL() + "\t" + doc.getContent().replace("\r\n", "") + "\r\n";

                writer.write(line);

            }

            writer.close();

            return true;

        } catch (IOException e) {

            e.printStackTrace();
        }

        return false;

    }

    /**
     * Clear the existing or previous tasks.
     * This should be called each time assigning tasks.
     */
    public void clearTasks() {

        this.docMap.clear();

    }

    // TODO: safe close all
    public void close() {

        try {

            this.docDB.close();
            this.urlToDocIDDB.close();
            this.docIDToURLDB.close();

            this.storedClassCatalog.close();

            this.env.close();

        } catch (DatabaseException de) {

            // TODO: update
            logger.debug("(DocumentTaskStorage) Get DatabaseException when closing the indexer db environment!");

        }

    }

}
