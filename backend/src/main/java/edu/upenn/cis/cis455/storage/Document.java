package edu.upenn.cis.cis455.storage;

import java.io.Serializable;
import java.util.Date;

public class Document implements Serializable {

    //////////////////////////////
    //// Work as a database entity,
    //// containing docID, url (parsed), content, last fetched datetime
    //////////////////////////////
    private final int id;
    private final String url;
    private final String content;
    private final String lastTime;

    /**
     * Constructor for an Document entity.
     * 
     * @param id          : [int], docID
     * @param url         : [String]
     * @param content     : [String]
     * @param visitedDate : [String], current visited date
     */
    public Document(int id, String url, String content, String visitedDate) {

        // set fields
        this.id = id;
        this.url = url;
        this.content = content;
        this.lastTime = visitedDate;

    }

    public int getID() {

        return this.id;

    }

    public String getURL() {

        return this.url;

    }

    public String getContent() {

        return this.content;

    }

    public String getLastVisitedDate() {

        return this.lastTime;

    }

    public String toString() {

        return "ID: " + this.id + " , URL: " + this.url + " , LastTime: " + this.lastTime;

    }

}
