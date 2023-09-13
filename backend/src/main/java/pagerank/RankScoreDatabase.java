package pagerank;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.io.*;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import spark.HaltException;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.collections.StoredEntrySet;
import com.sleepycat.collections.StoredSortedMap;


public class RankScoreDatabase implements Serializable{
	
	// Logger
	final static Logger logger = LogManager.getLogger(RankScoreDatabase.class);
	
	public static Environment BDBInstance = null;
	
	private static final String CLASS_CATALOG = "java_class_catalog";
	private StoredClassCatalog javaCatalog;
	private static final String PAGE_RANK = "PAGE_RANK";
	
    private Database rankScoreDb;
    private StoredSortedMap<Integer, Double> rankScoreMap;
    
	/**
	 * Constructor
	 */
	public RankScoreDatabase(String directory) throws FileNotFoundException{
		try {
			
		 logger.debug("Opening environment in: " + directory); 
		 
		 // Create Environment (Database)
   		 EnvironmentConfig envConfig = new EnvironmentConfig();
   		 envConfig.setTransactional(true);
   		 envConfig.setAllowCreate(true);
   		 BDBInstance = new Environment(new File(directory), envConfig);
   		 
   		 logger.debug("Successfully created Database.");
   		   		 
   		 // Create Databases (Tables)
   		 DatabaseConfig dbConfig = new DatabaseConfig();
   		 dbConfig.setTransactional(true);
   		 dbConfig.setAllowCreate(true);
   		 
   		 Database catalogDb = BDBInstance.openDatabase(null, CLASS_CATALOG, dbConfig);
   		 javaCatalog 		= new StoredClassCatalog(catalogDb);
   		rankScoreDb 		= BDBInstance.openDatabase(null, PAGE_RANK, dbConfig);
   		 
   		 // Generate Bindings & Maps
   		 EntryBinding<Integer> docID 	= new SerialBinding<Integer>(javaCatalog, Integer.class);
   		 EntryBinding<Double> rankScore = new SerialBinding<Double>(javaCatalog, Double.class);  		 
   		 
   		 rankScoreMap	= new StoredSortedMap<Integer, Double>(this.getTable(), docID, rankScore, true);
   		 
   		} catch (DatabaseException dbe) {
   			logger.error("Exception " + dbe + "encountered while creating StorageDatabase Instance.");
   		} 
	}
    /**
     * How many documents so far?
     */
    public int getCorpusSize() {
    	return this.rankScoreMap.size();
    }

    /**
     * Add a new documentID with it's PageRank score
     */
    public void addTuple(int docID, double score) {
    	Map<Integer, Double> entries = this.getScoreMap();
    	entries.put(docID, score);
    }
    
    /**
     * Shuts down / flushes / closes the storage system
     */
    public void close() {
    	try {
    		 if (BDBInstance != null) {
    			 rankScoreDb.close();
    			 javaCatalog.close();
    			 BDBInstance.close();
    			 }
    		} catch (DatabaseException dbe) {
    			logger.error("Exception " + dbe + "encountered while closing StorageDatabase Instance.");
    		} 

    }
    
    ///////////////////////////////////////////////////////////////
    ///////////// Instance Retrieval Methods //////////////////////
    ///////////////////////////////////////////////////////////////
    
    
    public final StoredSortedMap<Integer, Double> getScoreMap(){
        return rankScoreMap;
    }
    
    public final Database getTable(){
        return rankScoreDb;
    }
    
    public final StoredClassCatalog getCatalog() {
    	return javaCatalog;
    }
    
    public final Environment getEnvironment()
    {
        return BDBInstance;
    }
}