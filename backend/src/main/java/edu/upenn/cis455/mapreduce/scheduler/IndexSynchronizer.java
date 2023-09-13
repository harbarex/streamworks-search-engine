package edu.upenn.cis455.mapreduce.scheduler;

import java.util.ArrayList;
import java.util.HashMap;

import javax.imageio.plugins.tiff.TIFFDirectory;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.github.cdimascio.dotenv.Dotenv;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.collections.StoredSortedMap;

import edu.upenn.cis455.mapreduce.worker.storage.IndexStorage;
import edu.upenn.cis455.mapreduce.worker.storage.StorageFactory;
import edu.upenn.cis455.mapreduce.worker.storage.entities.DocInfo;
import edu.upenn.cis455.mapreduce.worker.storage.entities.DocWord;
import edu.upenn.cis455.mapreduce.worker.storage.entities.WordHit;
import edu.upenn.cis455.mapreduce.worker.storage.entities.Word;

import indexer.IndexMySQLStorage;
import storage.MySQLConfig;

public class IndexSynchronizer {

    final static Logger logger = LogManager.getLogger(IndexSynchronizer.class);

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
    private int batchSize = 2000;

    boolean syncLexicon = false;
    boolean syncDocWord = false;
    boolean syncDocInfo = false;
    boolean doTfIdf = false;
    boolean cleanUpdates = false;

    public IndexSynchronizer(String indexDirectory) {

        // init
        this.indexDB = StorageFactory.getIndexDatabase(indexDirectory);
        this.remoteDB = new IndexMySQLStorage(getMySQLConfig());

    }

    public IndexSynchronizer(String indexDirectory, int batchSize) {

        this.batchSize = batchSize;

        // init
        this.indexDB = StorageFactory.getIndexDatabase(indexDirectory);

        this.remoteDB = new IndexMySQLStorage(getMySQLConfig());

    }

    public IndexSynchronizer(String indexDirectory, int batchSize, boolean syncLexicon, boolean syncDocWord,
            boolean syncDocInfo) {

        this.batchSize = batchSize;

        // init
        this.indexDB = StorageFactory.getIndexDatabase(indexDirectory);

        this.remoteDB = new IndexMySQLStorage(getMySQLConfig());

        this.syncLexicon = syncLexicon;
        this.syncDocWord = syncDocWord;
        this.syncDocInfo = syncDocInfo;

    }

    public IndexSynchronizer(String indexDirectory, int batchSize, boolean syncLexicon, boolean syncDocWord,
            boolean syncDocInfo, boolean doTfIdf) {

        this.batchSize = batchSize;

        // init
        this.indexDB = StorageFactory.getIndexDatabase(indexDirectory);

        this.remoteDB = new IndexMySQLStorage(getMySQLConfig());

        this.syncLexicon = syncLexicon;
        this.syncDocWord = syncDocWord;
        this.syncDocInfo = syncDocInfo;
        this.doTfIdf = doTfIdf;

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

            String[] collections = { "short" };
            String[] tables = { shortDocWordsTable };

            for (int i = 0; i < collections.length; i++) {

                StoredSortedMap docWordMap = indexDB.getDocWordMap(collections[i]);

                ArrayList<DocWord> tasks = new ArrayList<DocWord>();

                int curr = 0;

                for (Object value : docWordMap.values()) {

                    DocWord docW = (DocWord) value;

                    tasks.add(docW);

                    if (tasks.size() == batchSize) {

                        // upload
                        remoteDB.insertBulkDocWords(tasks, tables[i]);

                        logger.info("Sync DocWord - current at " + curr + " over " + docWordMap.size());

                        tasks.clear();

                    }

                    curr++;

                }

                // deal with the rest
                if (tasks.size() > 0) {

                    remoteDB.insertBulkDocWords(tasks, tables[i]);

                    logger.info("Sync DocWord - last batch!");

                    tasks.clear();

                }

            }

        }

    }

    private class SyncDocInfo implements Runnable {

        @Override
        public void run() {

            StoredSortedMap docInfoMap = indexDB.getDocInfoMap();

            ArrayList<DocInfo> infos = new ArrayList<DocInfo>();

            for (Object value : docInfoMap.values()) {

                DocInfo info = (DocInfo) value;

                infos.add(info);

                if (infos.size() == batchSize) {

                    remoteDB.insertBulkDocInfo(infos, docInfoTable);

                    infos.clear();

                }

            }

            if (infos.size() > 0) {

                remoteDB.insertBulkDocInfo(infos, docInfoTable);
                infos.clear();

            }

        }

    }

    private class SyncLexicon implements Runnable {

        @Override
        public void run() {

            String[] collections = { "short" };
            String[] tables = { shortLexiconTable, plainLexiconTable };

            for (int i = 0; i < collections.length; i++) {

                // HashMap<Integer, Word> tasks = indexDB.getLexiconUploadTasks(batchSize,
                // collections[i]);
                StoredSortedMap lexiconMap = indexDB.getLexiconMap(collections[i]);

                ArrayList<Word> words = new ArrayList<Word>();

                int curr = 0;

                for (Object w : lexiconMap.values()) {

                    Word wd = (Word) w;

                    words.add(wd);

                    if (words.size() == batchSize) {

                        remoteDB.insertBulkLexicon(words, tables[i]);

                        logger.info("Sync lexicon - current at " + curr + " over " + lexiconMap.size());

                        words.clear();
                    }

                    curr++;

                }

                if (words.size() > 0) {

                    remoteDB.insertBulkLexicon(words, tables[i]);

                    logger.info("Sync lexicon - last batch!");

                    words.clear();

                }

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

        IndexSynchronizer scheduler;

        if (args.length == 1) {

            scheduler = new IndexSynchronizer(args[0]);
            scheduler.execute();

        } else if (args.length == 2) {

            scheduler = new IndexSynchronizer(args[0], Integer.parseInt(args[1]));
            scheduler.execute();

        } else if (args.length == 5) {

            scheduler = new IndexSynchronizer(args[0], Integer.parseInt(args[1]),
                    Boolean.parseBoolean(args[2]), Boolean.parseBoolean(args[3]), Boolean.parseBoolean(args[4]));
            scheduler.execute();

        } else if (args.length == 6) {

            scheduler = new IndexSynchronizer(args[0], Integer.parseInt(args[1]),
                    Boolean.parseBoolean(args[2]), Boolean.parseBoolean(args[3]), Boolean.parseBoolean(args[4]),
                    Boolean.parseBoolean(args[5]));
            scheduler.execute();

        }

    }

}
