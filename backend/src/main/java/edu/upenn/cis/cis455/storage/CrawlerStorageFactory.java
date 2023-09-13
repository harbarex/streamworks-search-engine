package edu.upenn.cis.cis455.storage;

import java.util.HashMap;

import edu.upenn.cis.cis455.storage.CrawlerDatabase;

public class CrawlerStorageFactory {

    // singleton
    private static HashMap<String, StorageInterface> dbInstances = new HashMap<String, StorageInterface>();

    public static StorageInterface getDatabaseInstance(String directory) {

        // factory object, instantiate your storage server
        if (!dbInstances.containsKey(directory)) {

            dbInstances.put(directory, new CrawlerDatabase(directory));

        }

        return dbInstances.get(directory);

    }
}
