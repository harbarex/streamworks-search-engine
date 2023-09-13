package edu.upenn.cis455.mapreduce.scheduler;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.github.cdimascio.dotenv.Dotenv;

import edu.upenn.cis455.mapreduce.worker.storage.IndexStorage;
import edu.upenn.cis455.mapreduce.worker.storage.StorageFactory;
import edu.upenn.cis455.mapreduce.worker.storage.entities.DocInfo;
import edu.upenn.cis455.mapreduce.worker.storage.entities.DocWord;
import edu.upenn.cis455.mapreduce.worker.storage.entities.WordHit;
import edu.upenn.cis455.mapreduce.worker.storage.entities.Word;

import indexer.IndexMySQLStorage;
import storage.MySQLConfig;

public class IndexSyncScheduler {

    final static Logger logger = LogManager.getLogger(IndexSyncScheduler.class);

    private static Dotenv dotenv = Dotenv.configure().load();

    private String shortLexiconTable = dotenv.get("INDEXER_SHORT_LEXICON");
    private String plainLexiconTable = dotenv.get("INDEXER_PLAIN_LEXICON");
    private String shortDocWordsTable = dotenv.get("INDEXER_SHORT_DOCWORDS");
    private String plainDocWordsTable = dotenv.get("INDEXER_PLAIN_DOCWORDS");
    private String docInfoTable = dotenv.get("INDEXER_DOCINFO");

    /**
     * Schedule the synchronization of the indexed data to the remote storage.
     * Handle two threads :
     * (1) Lexicon
     * (2) DocWord
     * (3) DocInfo
     * After these two are done, update lexicon's idf with SQL query.
     */

    private IndexStorage indexDB;
    private IndexMySQLStorage remoteDB;
    private int batchSize = 1000;

    boolean syncLexicon = false;
    boolean syncDocWord = false;
    boolean syncDocInfo = false;
    boolean doTfIdf = false;
    boolean cleanUpdates = false;

    public IndexSyncScheduler(String indexDirectory) {

        // init
        this.indexDB = StorageFactory.getIndexDatabase(indexDirectory);
        this.remoteDB = new IndexMySQLStorage(getMySQLConfig());

    }

    public IndexSyncScheduler(String indexDirectory, int batchSize) {

        this.batchSize = batchSize;

        // init
        this.indexDB = StorageFactory.getIndexDatabase(indexDirectory);

        this.remoteDB = new IndexMySQLStorage(getMySQLConfig());

    }

    public IndexSyncScheduler(String indexDirectory, int batchSize, boolean syncLexicon, boolean syncDocWord,
            boolean syncDocInfo) {

        this.batchSize = batchSize;

        // init
        this.indexDB = StorageFactory.getIndexDatabase(indexDirectory);

        this.remoteDB = new IndexMySQLStorage(getMySQLConfig());

        this.syncLexicon = syncLexicon;
        this.syncDocWord = syncDocWord;
        this.syncDocInfo = syncDocInfo;

    }

    public IndexSyncScheduler(String indexDirectory, int batchSize, boolean syncLexicon, boolean syncDocWord,
            boolean syncDocInfo, boolean doTfIdf, boolean cleanUpdates) {

        this.batchSize = batchSize;

        // init
        this.indexDB = StorageFactory.getIndexDatabase(indexDirectory);

        this.remoteDB = new IndexMySQLStorage(getMySQLConfig());

        this.syncLexicon = syncLexicon;
        this.syncDocWord = syncDocWord;
        this.syncDocInfo = syncDocInfo;
        this.doTfIdf = doTfIdf;
        this.cleanUpdates = cleanUpdates;

    }

    private MySQLConfig getMySQLConfig() {
        return new MySQLConfig(
                dotenv.get("INDEXER_MYSQL_DBNAME"),
                dotenv.get("INDEXER_MYSQL_USERNAME"),
                dotenv.get("INDEXER_MYSQL_PASSWORD"),
                dotenv.get("INDEXER_MYSQL_HOSTNAME"),
                dotenv.get("INDEXER_MYSQL_PORT"));
    }

    // define runnable job to sync
    private class SyncDocWords implements Runnable {

        @Override
        public void run() {

            String[] collections = { "short", "plain" };
            String[] tables = { shortDocWordsTable, plainDocWordsTable };

            for (int i = 0; i < collections.length; i++) {

                HashMap<String, DocWord> tasks = indexDB.getDocWordUploadTasks(batchSize, collections[i]);

                while (tasks.size() > 0) {

                    remoteDB.insertAndUpdateDocWords(tasks, tables[i]);

                    indexDB.removeDocWordUploadTasks(tasks.keySet(), collections[i]);

                    tasks = indexDB.getDocWordUploadTasks(batchSize, collections[i]);

                }

            }

        }

    }

    private class SyncDocInfo implements Runnable {

        @Override
        public void run() {

            HashMap<Integer, DocInfo> infoTasks = indexDB.getDocInfoUploadTasks(batchSize);

            while (infoTasks.size() > 0) {

                remoteDB.insertDocInfo(infoTasks, docInfoTable);

                indexDB.removeDocInfoUploadTasks(infoTasks.keySet());

                infoTasks = indexDB.getDocInfoUploadTasks(batchSize);

            }

        }

    }

    private class SyncLexicon implements Runnable {

        @Override
        public void run() {

            String[] collections = { "short", "plain" };
            String[] tables = { shortLexiconTable, plainLexiconTable };

            for (int i = 0; i < collections.length; i++) {

                HashMap<Integer, Word> tasks = indexDB.getLexiconUploadTasks(batchSize, collections[i]);

                while (tasks.size() > 0) {

                    remoteDB.insertLexicon(tasks, tables[i]);

                    indexDB.removeLexiconUploadTasks(tasks.keySet(), collections[i]);

                    tasks = indexDB.getLexiconUploadTasks(batchSize, collections[i]);

                }

            }

            for (int i = 0; i < collections.length; i++) {

                // update lexicon
                ArrayList<Word> tasks = new ArrayList<Word>();

                for (int j = 0; j < indexDB.nextWordID.get(); j++) {

                    if (tasks.size() == batchSize) {

                        remoteDB.updateLexicon(tasks, tables[i]);

                        tasks.clear();

                    }

                    Word wd = indexDB.getLexicon(i, collections[i]);

                    if (wd != null) {

                        tasks.add(wd);

                    }

                }

                remoteDB.updateLexicon(tasks, tables[i]);

                tasks.clear();
            }

        }

    }

    public void execute() {

        indexDB.showIndexStorageStatistics(false);

        logger.info("Executing the synchronization with the remote database ... ");

        Thread docWordThread = null;
        Thread docInfoThread = null;
        Thread lexiconThread = null;

        if (this.syncDocWord) {

            docWordThread = new Thread(new SyncDocWords());

            docWordThread.start();

        }

        if (this.syncDocInfo) {

            docInfoThread = new Thread(new SyncDocInfo());

            docInfoThread.start();

        }

        if (this.syncLexicon) {

            lexiconThread = new Thread(new SyncLexicon());

            lexiconThread.start();

        }

        try {

            if (this.syncDocWord && docWordThread != null) {
                docWordThread.join();
            }

            if (this.syncDocInfo && docInfoThread != null) {
                docInfoThread.join();
            }

            if (this.syncLexicon && lexiconThread != null) {
                lexiconThread.join();
            }

        } catch (InterruptedException e) {

            e.printStackTrace();

        }

        if (this.doTfIdf) {
            // update tf-idf
            remoteDB.updateTFIDF(shortDocWordsTable, shortLexiconTable);
            remoteDB.updateTFIDF(plainDocWordsTable, plainLexiconTable);
        }

        if (this.cleanUpdates) {

            this.indexDB.clearAllUpdates();

        }

        remoteDB.close();
        indexDB.close();

        logger.info("Finished the synchronization!");

    }

    public static void main(String[] args) {

        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn", Level.DEBUG);
        org.apache.logging.log4j.core.config.Configurator.setLevel("indexer", Level.DEBUG);

        if (args.length < 1) {

            System.out.println(
                    "Usage: please at least provide 1 argument: [index storage path] [batch size] [syncLexicon] [syncDocword] [syncDocInfo]");
            return;

        }

        IndexSyncScheduler scheduler;

        if (args.length == 1) {

            scheduler = new IndexSyncScheduler(args[0]);
            scheduler.execute();

        } else if (args.length == 2) {

            scheduler = new IndexSyncScheduler(args[0], Integer.parseInt(args[1]));
            scheduler.execute();

        } else if (args.length == 5) {

            scheduler = new IndexSyncScheduler(args[0], Integer.parseInt(args[1]),
                    Boolean.parseBoolean(args[2]), Boolean.parseBoolean(args[3]), Boolean.parseBoolean(args[4]));
            scheduler.execute();

        } else if (args.length == 7) {

            scheduler = new IndexSyncScheduler(args[0], Integer.parseInt(args[1]),
                    Boolean.parseBoolean(args[2]), Boolean.parseBoolean(args[3]), Boolean.parseBoolean(args[4]),
                    Boolean.parseBoolean(args[5]), Boolean.parseBoolean(args[6]));
            scheduler.execute();

        }

    }

}
