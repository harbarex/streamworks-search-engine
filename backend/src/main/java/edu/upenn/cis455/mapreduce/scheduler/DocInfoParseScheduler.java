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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.jsoup.Jsoup;
// import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.regions.Region;

import edu.upenn.cis.cis455.storage.Document;
import edu.upenn.cis.cis455.storage.StorageInterface;
import edu.upenn.cis455.mapreduce.worker.storage.DocumentTaskStorage;
import edu.upenn.cis455.mapreduce.worker.storage.IndexStorage;
import edu.upenn.cis455.mapreduce.worker.storage.StorageFactory;

public class DocInfoParseScheduler {

    static Logger logger = LogManager.getLogger(DocInfoParseScheduler.class);

    // TODO: shift to DynamoDB
    private DocumentTaskStorage taskDB;
    private IndexStorage indexDB;

    public DocInfoParseScheduler(String taskSourceDirectory, String indexStorageDirectory) {

        this.taskDB = StorageFactory.getDocumentTaskDatabase(taskSourceDirectory);
        this.indexDB = StorageFactory.getIndexDatabase(indexStorageDirectory);

    }

    public void parseAndUpdateDocInfo() {

        ListIterator<Integer> taskIter = this.taskDB.getToDoTaskIDs();

        while (taskIter.hasNext()) {

            int docID = taskIter.next();

            logger.debug("DocInfoParser is parsing the docID : " + docID + " ...");

            edu.upenn.cis.cis455.storage.Document doc = this.taskDB.getDocumentByID(docID);

            // fetch title
            String title = getTitle(doc);
            String context = getContext(doc);
            String url = doc.getURL();

            if (!this.indexDB.hasSeenDocument(docID)) {

                logger.debug("(DocInfoParser) Get ID: " + docID + ", title: " + title + " , url: " + url);

                // save to index
                this.indexDB.addDocInfo(docID, url, title, context);

                this.indexDB.addDocumentID(docID);

            }

        }

        // show statistics
        this.indexDB.showIndexStorageStatistics(false);

        // close
        this.taskDB.close();
        this.indexDB.close();

    }

    public String handleTitle(String title) {

        // deal with potential erros
        int maxLength = Math.min(150, title.length());

        String refinedTitle = title.substring(0, maxLength);

        // remove bad words like \ & "
        refinedTitle = refinedTitle.replace("\"", "\\\"");
        refinedTitle = refinedTitle.replace("\\", "\\\\");

        return refinedTitle;

    }

    public String getTitle(edu.upenn.cis.cis455.storage.Document doc) {

        org.jsoup.nodes.Document jdoc = Jsoup.parse(doc.getContent());

        String title = jdoc.title();

        if (title == null) {

            // look for h tags
            Elements hTags = jdoc.select("h1, h2, h3, h4, h5, h6");

            for (int i = 0; i < hTags.size(); i++) {

                Element h = hTags.get(i);

                if (h.text() != null) {

                    title = h.text();
                    break;

                }

            }

        }

        if (title != null) {

            title = handleTitle(title);

        }

        return title;

    }

    public String getContext(edu.upenn.cis.cis455.storage.Document doc) {

        org.jsoup.nodes.Document jdoc = Jsoup.parse(doc.getContent());

        String context = null;

        Elements tags = jdoc.select("p");

        for (Element tag : tags) {

            if (tag != null && tag.text() != null) {

                String ctx = tag.text().strip();

                if (ctx.length() > 0) {

                    context = tag.text();

                    break;

                }

            }

        }

        // handle context
        if (context != null) {

            context = context.substring(0, Math.min(250, context.length()));

        } else {

            context = "";

        }

        return context;

    }

    public static void main(String[] args) {

        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn", Level.DEBUG);

        if (args.length < 2) {

            System.out.println(
                    "Usage: please provide 2 arguments: [task source directory] [index storage directory]");

            return;

        }

        DocInfoParseScheduler scheduler = new DocInfoParseScheduler(args[0], args[1]);
        scheduler.parseAndUpdateDocInfo();

    }

}
