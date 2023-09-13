package edu.upenn.cis.cis455.storage;

import java.io.Serializable;
import java.util.Date;

public class ContentDigest implements Serializable {

    //////////////////////////////
    //// Work as a database entity,
    //// containing content digest & original content
    //////////////////////////////
    private final String digest;
    private final String content;
    // private final long lastCrawlerRunID; // in milliseconds as Run ID

    /**
     * Constructor for an ContentDigest entity.
     * 
     * @param digest       : [String], MD5-hashed content
     * @param content      : [String], original content
     * @param crawlerRunID (deprecated) : [long], use date in milliseconds at the
     *                     run ID
     */
    public ContentDigest(String digest, String content) {

        // set fields
        this.digest = digest;

        this.content = content;

        // this.lastCrawlerRunID = crawlerRunID;

    }

    public String getDigest() {

        return this.digest;
    }

    public String getContent() {

        return this.content;

    }

    // public long getLastCrawlerRunID() {

    // return this.lastCrawlerRunID;

    // }

    public String toString() {

        return "Digest: " + this.digest;

    }

}
