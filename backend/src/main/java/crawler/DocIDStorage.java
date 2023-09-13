package crawler;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * Berkeley DB- backed storage for storing and assigning consistent integer IDs to documents
 *  after the crawl is complete.
 * @author Daniel
 *
 */
public class DocIDStorage {
    
    final static String CLASS_CATALOG_DB_NAME = "CATALOG_DB";
    final static String DOC_ID_DB_NAME = "DOC_ID_DB";
    
    Map<String, Integer> docIDs;
    
    StoredClassCatalog catalog;
    Environment env;
    
    Database docIDsDB;
    
    
    public DocIDStorage(String dir) {
        createIfNotExists(dir);
        EnvironmentConfig eConfig = new EnvironmentConfig();
        eConfig.setAllowCreate(true);
        File dbFile = new File(dir);
        env = new Environment(dbFile, eConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        
        Database catalogDB = env.openDatabase(null, CLASS_CATALOG_DB_NAME, dbConfig);
        catalog = new StoredClassCatalog(catalogDB);
        
        docIDsDB = env.openDatabase(null, DOC_ID_DB_NAME, dbConfig);
        docIDs = getDBMap(docIDsDB, String.class, Integer.class);
    }
    
    public synchronized void createIfNotExists(String directory) {
        File d = new File(directory);
        if (!d.exists()) {
            d.mkdir();
        }
    }
    
    protected <K, V> StoredSortedMap<K, V> getDBMap(Database db, Class<K> keyClass, Class<V> valueClass) {
        EntryBinding<K> keyBinding = new SerialBinding<K>(catalog, keyClass);
        EntryBinding<V> valueBinding = new SerialBinding<V>(catalog, valueClass);
        return new StoredSortedMap<K, V>(db, keyBinding, valueBinding, true);
    }
    
    /**
     * Retrieves an integer id for the given url, or generates a new id if url not seen before.
     * @param url
     * @return
     */
    public synchronized int getOrGenerateID(String url) {
        URLInfo urlInfo = new URLInfo(url);
        if (docIDs.containsKey(urlInfo.toString())) {
            return docIDs.get(urlInfo.toString());
        }
        int id = docIDs.size();
        docIDs.put(urlInfo.toString(), id);
        return id;
    }
    
    /**
     * Retrieves an integer id for the given url, or returns null if url not seen before.
     * @param url
     * @return
     */
    public synchronized Integer getID(String url) {
        URLInfo urlInfo = new URLInfo(url);
        return docIDs.get(urlInfo.toString());
    }
    
    /**
     * Sets the id for the given url
     * @param url
     * @param id
     */
    public synchronized void setID(String url, int id) {
        URLInfo urlInfo = new URLInfo(url);
        docIDs.put(urlInfo.toString(), id);
    }
            
    public synchronized void close() {
        docIDsDB.close();
        
        catalog.close();
        env.close();
    }
}
