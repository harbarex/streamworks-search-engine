package edu.upenn.cis.stormlite.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;
import org.jsoup.Jsoup;
// import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.io.IOException;
import java.lang.System.Logger;

import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.spout.FileSpout;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;
import edu.upenn.cis455.mapreduce.worker.storage.DocumentTaskStorage;
import edu.upenn.cis455.mapreduce.worker.storage.StorageFactory;
import edu.upenn.cis.cis455.storage.StorageInterface;

// import edu.upenn.cis.cis455.storage.Document;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis.stormlite.spout.URLInfo;

public class ShortFileSpout extends FileSpout {

    /**
     * This FileSpout addresses short texts (e.g. title, anchor) in a html file.
     * (TODO: address short section (abstract) of a PDF)
     */

    private String targetInputDirectory;
    private String workerIndex;

    // local db to get documents
    DocumentTaskStorage taskDB;

    // iterator for spout.execute
    ListIterator<Integer> taskIter;
    Iterator<String[]> lineIter = null;

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {

        this.collector = collector;

        // target subfolder
        String localStorage = (String) conf.get("storageDirectory");
        String inputDir = (String) conf.get("input");
        targetInputDirectory = WorkerServer.configureLocalSubDirectory(localStorage, inputDir, false);

        // task DB: targetInputDirectory
        // the one to retrieve document from
        // this should be initiated in specified input directory in MapReduce
        taskDB = StorageFactory.getDocumentTaskDatabase(targetInputDirectory);

        // worker index
        workerIndex = (String) conf.get("workerIndex");
        log.debug("(ShortSpout) Index: " + workerIndex + ", Target subdirectory: " + targetInputDirectory);

        // check number of tasks retrieved
        taskIter = taskDB.getToDoTaskIDs();
        log.debug("(ShortSpout) Get " + taskDB.getCurrentNumberOfTasks() + " tasks!");

    }

    /**
     * Get the original filename.
     * During open, each worker adds its Index
     * following the filename to read its sharded file.
     */
    @Override
    public String getFilename() {
        return null;
    }

    /**
     * Verify whether it is written in English or not.
     * 
     * @param jdoc
     * @return
     */
    public boolean isEnglishDoc(org.jsoup.nodes.Document jdoc) {

        String languge = jdoc.select("html").first().attr("lang");

        if (languge != null && (languge.startsWith("en"))) {

            return true;

        }

        return false;

    }

    /**
     * Retrieve needed info (lines) from HTML docs.
     * line format: [line, docID, tagName]
     * 
     * @param doc
     */
    public void getShortHTMLContents(edu.upenn.cis.cis455.storage.Document doc) {

        ArrayList<String[]> lines = new ArrayList<String[]>();

        org.jsoup.nodes.Document jdoc = Jsoup.parse(doc.getContent());

        // ignore non-english-based document
        if (!this.isEnglishDoc(jdoc)) {

            lineIter = lines.iterator();
            return;

        }

        // sequences => h, p / span, a

        // h tags
        Elements hTags = jdoc.select("h1, h2, h3, h4, h5, h6");

        for (int i = 0; i < hTags.size(); i++) {

            Element h = hTags.get(i);

            if (h.text() != null) {

                String[] values = { h.text(), "" + doc.getID(), h.nodeName().toLowerCase() + i };

                lines.add(values);

            }

        }

        // plain texts
        Elements paragraphs = jdoc.select("p,span");

        int nParagraphs = 4;

        for (int i = 0; i < nParagraphs; i++) {

            if (i >= paragraphs.size()) {

                break;

            }

            Element p = paragraphs.get(i);

            if (p.text() != null) {

                String[] values = { p.text(), "" + doc.getID(), p.nodeName().toLowerCase() + i };

                lines.add(values);

            }

        }

        Elements anchors = jdoc.select("a[href]");

        for (int i = 0; i < anchors.size(); i++) {

            Element anchor = anchors.get(i);

            String url = anchor.attr("href");

            if ((anchor.text() != null) && (url != null)) {

                // both of them exists => take them into consideration
                // get the toDocID of this url

                // refine the url if necessary by the fromURL
                String refinedURL = this.refineURL(doc.getURL(), url);

                int toDocID = this.taskDB.getIDByURL(refinedURL);

                if (toDocID >= 0) {

                    // note: anchor tag name => tag name_fromDoc_i
                    String[] values = { anchor.text(), "" + toDocID,
                            anchor.nodeName().toLowerCase() + "_" + doc.getID() + "_" + i };

                    lines.add(values);

                }

            }

        }

        // create iterator
        lineIter = lines.iterator();

    }

    /**
     * Normalize the URL from the html documents, based on the source URL
     * and the URLInfo class to prepend HTTP & port
     * 
     * @param fromURL
     * @param toURL
     * @return
     */
    private String refineURL(String parentURL, String extURL) {

        // has its own domain
        if (extURL.startsWith("http://") || extURL.startsWith("https://")) {

            // extractedURL.add(new URLInfo(extURL));
            URLInfo urlInfo = new URLInfo(extURL);

            return urlInfo.toString();

        } else {

            // no domain => get domain or parent path
            // Some parent url may end with a filename (e.g. xxxxx/index.html)
            String lastPart = parentURL.substring(parentURL.lastIndexOf("/"));

            parentURL = (lastPart.contains(".")) ? parentURL.substring(0, parentURL.lastIndexOf("/") + 1)
                    : parentURL;

            parentURL = parentURL.endsWith("/") ? parentURL : parentURL + "/";

            // start with "/" => remove it
            extURL = extURL.startsWith("/") ? extURL.substring(1) : extURL;

            // extractedURL.add(new URLInfo(parentURL + extURL));
            URLInfo urlInfo = new URLInfo(parentURL + extURL);

            return urlInfo.toString();

        }

    }

    @Override
    public synchronized boolean nextTuple() {

        if (((taskIter.hasNext()) || ((lineIter != null) && (lineIter.hasNext()))) & !sentEof) {

            if (((lineIter != null) && (lineIter.hasNext()))) {

                String[] pair = lineIter.next();

                this.collector.emit(new Values<Object>(String.valueOf(inx++), pair),
                        getExecutorId());

            } else if (taskIter.hasNext()) {

                log.debug("(ShortSpout) worker : " + workerIndex + " is handling task : " + taskIter.nextIndex());

                edu.upenn.cis.cis455.storage.Document doc = this.taskDB.getDocumentByID(taskIter.next());

                // process doc
                getShortHTMLContents(doc);

            }

            Thread.yield();
            return true;

        } else if (!sentEof) {

            log.info(getExecutorId() + " finished file " + getFilename() + " and emitting EOS");
            this.collector.emitEndOfStream(getExecutorId());

            taskIter = null;
            sentEof = true;

            return false;

        }

        return false;

    }

}
