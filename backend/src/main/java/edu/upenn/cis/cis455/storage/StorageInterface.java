package edu.upenn.cis.cis455.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.upenn.cis.cis455.crawler.utils.URLInfo;

public interface StorageInterface {

  /**
   * 
   */

  public ArrayList<Integer> exportDocuments(int start, int end);

  public Document getDocumentByID(int docID);

  public String getURLByDocID(int docID);

  public String getContent(String url);

  /**
   * How many documents so far?
   */
  public int getCorpusSize();

  /**
   * Add a new document, getting its ID
   */
  public int addDocument(String url, String documentContents);

  /**
   * Retrieves a document's contents by URL
   */
  public String getDocument(String url);

  /**
   * Adds a user and returns an ID
   */
  public int addUser(String username, String password);

  /**
   * Tries to log in the user, or else throws a HaltException
   */
  public boolean getSessionForUser(String username, String password);

  /**
   * Shuts down / flushes / closes the storage system
   */
  public void close();

  /**
   * (Additionally added)
   * Check user exists or not
   */
  public boolean userExists(String username);

  /**
   * (Additionally added)
   * URL seen or not
   */
  public boolean shouldAddDocument(String url, String lastModified);

  /**
   * (Additionally added)
   * content seen or not
   */
  public boolean isContentSeen(String digest);

  /**
   * (Additionally added)
   * Add content digest
   */
  public void addContentDigest(String digest, String content);

  /**
   * (Added for Channel)
   * Create a channel.
   * true: successfully created;
   * false: fail to create (mainly due to duplicate channel name).
   */
  public boolean createChannel(String channelName, String username, String pattern);

  /**
   * (Added for Channel)
   * Add a document URL into a specific channel
   */
  public void addURLToChannel(String channelName, String url);

  /**
   * (Added for Channel Show)
   * Fetch the specified channel from the database
   */
  public Channel getChannel(String channelName);

  /**
   * (Added for Channel Preview)
   */
  public ArrayList<String> getAllChannels();

  /**
   * (Added for Channel Rendering)
   * Retrieves a document object
   */
  public Document getDocumentEntity(String url);

  public HashMap<String, ArrayList<String>> getXPathToChannelMap();

  /**
   * (Added for URL Frontier)
   * Retrive (poll) an item from frontier
   * 
   */
  public ArrayList<URLInfo> getNextURLs();

  /**
   * (Added for URL frontier)
   * Append an item to frontier
   */
  public void addURLToFrontier(URLInfo urlInfo);

  /**
   * (Added for URL frontier)
   * Get current queue size
   */
  public int getFrontierSize();

  /**
   * (Added for StormState)
   * Update the working state of an id
   */
  public void updateStormState(String id, Boolean working);

  /**
   * (Added for StormState)
   * Check whether spouts & bolts are stopped or not
   */
  public boolean isStormWorking();

  /**
   * (Added for document counter per run)
   * Increase the document count of each run of each crawler
   */
  public void incDocCount(String masterID);

  /**
   * (Added for document counter per run)
   * To know the n files fetched in this run
   * 
   * @param masterID
   */
  public int getDocCount(String masterID);

  /**
   * To know content seen in each run
   * 
   */
  public int getContentSeenCount();

  /**
   * (Added for temporary db)
   * This includes frontier, content digest
   */
  public void clearTemporaryStorage();

}
