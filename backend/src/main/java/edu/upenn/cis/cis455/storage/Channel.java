package edu.upenn.cis.cis455.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;

public class Channel implements Serializable {

    //////////////////////////////
    //// Work as a database entity,
    //// containing username, createdDate, docURLs
    //////////////////////////////
    private final String channelName;
    private final String username;
    private final String createdDate;
    private final String pattern;
    public HashSet<String> docURLs = new HashSet<String>();

    /**
     * The constructor to instantiate a channel entity.
     * 
     * @param channelName
     * @param username
     * @param createdDate
     * @param pattern
     */
    public Channel(String channelName, String username, String createdDate, String pattern) {

        this.channelName = channelName;

        this.username = username;

        this.createdDate = createdDate;

        this.pattern = pattern;

    }

    public String getUsername() {

        return this.username;

    }

    public String getCreatedDate() {

        return this.createdDate;

    }

    public int getNumberOfDocs() {

        return this.docURLs.size();

    }

    public String getPattern() {

        return this.pattern;

    }

    public String toString() {

        return "Channel Name: " + channelName + " , Created By: " + this.username + " , At: " + this.createdDate;

    }

}
