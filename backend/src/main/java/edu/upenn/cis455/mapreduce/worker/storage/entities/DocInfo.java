package edu.upenn.cis455.mapreduce.worker.storage.entities;

import java.io.Serializable;
import java.util.Date;

public class DocInfo implements Serializable {

    private int docID;
    private String url;
    private String title;
    private String context;

    /**
     * 
     * @param docID
     * @param url
     * @param title
     */
    public DocInfo(int docID, String url, String title, String context) {

        // set fields
        this.docID = docID;
        this.url = url;
        this.title = title;
        this.context = context;

    }

    public int getDocID() {

        return this.docID;

    }

    public String getURL() {

        return this.url;

    }

    public String getTitle() {

        return this.title;

    }

    public String getContext() {

        return this.context;

    }

    // TODO: add query string for sync

    public String toString() {

        String printTitle = title;

        if (title.contains("\\")) {

            printTitle = printTitle.replace("\\", "\\\\");

        }

        if (title.contains("\"")) {

            printTitle = printTitle.replace("\"", "\\\"");
        }

        String printContext = context;
        if (context.contains("\\")) {

            printContext = printContext.replace("\\", "\\\\");

        }

        if (context.contains("\"")) {

            printContext = printContext.replace("\"", "\\\"");
        }

        return "(" + docID + "," + "\"" + url + "\"" + "," + "\"" + printTitle + "\"" + "," + "\"" + printContext + "\""
                + ")";

    }

}
