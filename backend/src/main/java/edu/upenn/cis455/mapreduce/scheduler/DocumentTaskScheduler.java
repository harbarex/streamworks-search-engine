package edu.upenn.cis455.mapreduce.scheduler;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;

// import edu.upenn.cis.cis455.storage.CrawlerStorageFactory;
// import edu.upenn.cis.cis455.storage.Document;
import edu.upenn.cis.cis455.storage.StorageInterface;
import edu.upenn.cis455.mapreduce.worker.storage.DocumentTaskStorage;
import edu.upenn.cis455.mapreduce.worker.storage.StorageFactory;

import storage.DynamoStorage;

// import api.data.Document;

public class DocumentTaskScheduler {

    final static Logger logger = LogManager.getLogger(DocumentTaskScheduler.class);

    // TODO: change to dynamoDB

    private int batchSize = 1000;

    public String[] paths;

    public ArrayList<api.data.Document> fetchDocuments(int start, int end) {

        DynamoDbEnhancedClient client = DynamoStorage.getDynamoDbEnhancedClient();

        return DynamoStorage.queryDocuments(client, start, end);

    }

    public ArrayList<DocumentTaskStorage> getTaskReceivers() {

        ArrayList<DocumentTaskStorage> receivers = new ArrayList<DocumentTaskStorage>();

        for (String path : paths) {

            DocumentTaskStorage receiver = StorageFactory.getDocumentTaskDatabase(path);

            receiver.clearTasks();

            receivers.add(receiver);

        }

        return receivers;

    }

    public void syncURLsToDocIDs(int startFrom, int end) {

        ArrayList<DocumentTaskStorage> receivers = this.getTaskReceivers();

        logger.info("(DocumentTaskScheduler) Synchronizing the URLs to DocIDs ... start from " + startFrom);

        int i = startFrom;

        while (true) {

            if (i >= end) {

                break;

            }

            int batchEnd = Math.min(i + batchSize, end);

            ArrayList<api.data.Document> docs = this.fetchDocuments(i, batchEnd);

            for (api.data.Document doc : docs) {

                String url = doc.getUrl();
                int docID = doc.getDocId();

                for (DocumentTaskStorage receiver : receivers) {

                    receiver.syncURLToDocID(url, docID);

                }

            }

            if ((batchEnd > 0) && ((batchEnd - startFrom) % batchSize == 0)) {

                logger.info(
                        "(DocumentTaskScheduler) Finished another " + batchSize
                                + " urls to docIDs sychronization! Current at " + batchEnd + " !");

            }

            i = batchEnd;

        }

        for (DocumentTaskStorage receiver : receivers) {

            receiver.showCurrentURLsToDocIDsSize();

        }

        logger.info("(DocumentTaskScheduler) Done with synchroniztion (URLs to DocIDs)!");

    }

    /**
     * 
     * @param start : [int]
     * @param end   : [int]
     */
    public void assignTasks(int start, int end) {

        ArrayList<api.data.Document> docs = this.fetchDocuments(start, end);

        ArrayList<DocumentTaskStorage> receivers = this.getTaskReceivers();

        int nReceivers = receivers.size();

        for (api.data.Document doc : docs) {

            int id = doc.getDocId();

            // convert to api.data.Document to edu.upenn.cis.cis455.storage.Document
            edu.upenn.cis.cis455.storage.Document toDoc = new edu.upenn.cis.cis455.storage.Document(id,
                    doc.getUrl(), doc.getContent(), "");

            receivers.get((id % nReceivers)).addNewDocumentTask(id, toDoc);

        }

        for (DocumentTaskStorage receiver : receivers) {

            logger.debug("(DocumentTaskScheduler) Assign " + receiver.getCurrentNumberOfTasks() + " (n) tasks!");

            receiver.close();

        }

    }

    // simulate task scheduler (using berkeleyDB)
    // (Deprecated) from BerkeleyDB
    // private static StorageInterface source =
    // CrawlerStorageFactory.getDatabaseInstance("./crawler");

    // public ArrayList<Integer> fetchTasks(int start, int end) {

    // // connect to the source
    // return source.exportDocuments(start, end);

    // }

    // public void assignTasks(int start, int end) {

    // ArrayList<Integer> docIDs = this.fetchTasks(start, end);

    // ArrayList<DocumentTaskStorage> receivers = this.getTaskReceivers();

    // int nReceivers = receivers.size();

    // for (int id : docIDs) {

    // Document doc = source.getDocumentByID(id);

    // if (doc == null) {

    // continue;

    // }

    // receivers.get((id % nReceivers)).addNewDocumentTask(doc.getID(), doc);

    // }

    // for (DocumentTaskStorage receiver : receivers) {

    // logger.debug("(DocumentTaskScheduler) Assign " +
    // receiver.getCurrentNumberOfTasks() + " (n) tasks!");

    // receiver.close();

    // }

    // source.close();

    // }

    // public void syncURLsToDocIDs() {

    // ArrayList<DocumentTaskStorage> receivers = this.getTaskReceivers();

    // int nDocs = source.getCorpusSize();

    // logger.info("(DocumentTaskScheduler) Synchronizing the URLs to DocIDs ...");

    // for (int i = 0; i < nDocs; i++) {

    // String url = source.getURLByDocID(i);

    // if (url == null) {

    // continue;

    // }

    // for (DocumentTaskStorage receiver : receivers) {

    // receiver.syncURLToDocID(url, i);

    // }

    // if (i % 2000 == 0) {

    // logger.info("(DocumentTaskScheduler) Finished another 2000 urls to docIDs
    // sychronization!");

    // }

    // }

    // for (DocumentTaskStorage receiver : receivers) {

    // receiver.showCurrentURLsToDocIDsSize();

    // }

    // logger.info("(DocumentTaskScheduler) Done with synchroniztion (URLs to
    // DocIDs)!");

    // }

    public static void main(String[] args) {

        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn", Level.DEBUG);
        org.apache.logging.log4j.core.config.Configurator.setLevel("storage", Level.DEBUG);

        if (args.length < 5) {

            System.out.println(
                    "Usage: please provide 3 arguments: [sync URL from] [sync URL end] [start] [end] [task receiver path1] [path2] [path3] ... [path_i]");
            return;

        }

        // boolean syncURL = Boolean.parseBoolean(args[0]);
        int syncURLStart = Integer.parseInt(args[0]);
        int syncURLEnd = Integer.parseInt(args[1]);
        int start = Integer.parseInt(args[2]);
        int end = Integer.parseInt(args[3]);

        DocumentTaskScheduler scheduler = new DocumentTaskScheduler();

        String[] targetPaths = new String[args.length - 4];
        for (int i = 0; i < args.length - 4; i++) {
            targetPaths[i] = args[4 + i];
        }

        scheduler.paths = targetPaths;

        if (syncURLStart >= 0) {

            scheduler.syncURLsToDocIDs(syncURLStart, syncURLEnd);

        }

        if (start >= 0) {

            scheduler.assignTasks(start, end);

        }

    }

}
