package edu.upenn.cis455.mapreduce.worker.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import edu.upenn.cis455.mapreduce.worker.storage.WorkerStorage;

public class StorageFactory {

    private static ConcurrentHashMap<String, DocumentTaskStorage> taskInstances = new ConcurrentHashMap<String, DocumentTaskStorage>();

    private static ConcurrentHashMap<String, WorkerStorage> dbInstances = new ConcurrentHashMap<String, WorkerStorage>();

    private static ConcurrentHashMap<String, HitStorage> hitInstances = new ConcurrentHashMap<String, HitStorage>();

    private static ConcurrentHashMap<String, IndexStorage> indexInstances = new ConcurrentHashMap<String, IndexStorage>();

    public static WorkerStorage getDatabase(String directory) {

        // check directory exist or not
        createDirectory(directory);

        // factory object, instantiate your storage server
        if (!dbInstances.containsKey(directory)) {

            dbInstances.put(directory, new WorkerStorage(directory));

        }

        return dbInstances.get(directory);

    }

    public static IndexStorage getIndexDatabase(String directory) {

        createDirectory(directory);

        if (!indexInstances.containsKey(directory)) {
            indexInstances.put(directory, new IndexStorage(directory));
        }

        return indexInstances.get(directory);

    }

    public static HitStorage getHitDatabase(String directory) {

        createDirectory(directory);

        if (!hitInstances.containsKey(directory)) {
            hitInstances.put(directory, new HitStorage(directory));
        }

        return hitInstances.get(directory);

    }

    public static DocumentTaskStorage getDocumentTaskDatabase(String directory) {

        createDirectory(directory);

        if (!taskInstances.containsKey(directory)) {

            taskInstances.put(directory, new DocumentTaskStorage(directory));

        }

        return taskInstances.get(directory);

    }

    public static void createDirectory(String directory) {

        if (!Files.exists(Paths.get(directory)))

        {

            try {

                Files.createDirectories(Paths.get(directory));

            } catch (IOException e) {

                e.printStackTrace();

            }
        }

    }

    public static void removeWorkerStorage(String directory) {

        // remove from dbInstances
        if (dbInstances.containsKey(directory)) {

            dbInstances.remove(directory);

        }

        // if exist => start deletion
        if (Files.exists(Paths.get(directory))) {

            File dbFile = new File(directory);

            deleteDirectory(dbFile);

            dbFile.delete();

        }

    }

    public static void deleteDirectory(File dbFile) {

        // store all the paths of files and folders present
        // inside directory
        for (File subfile : dbFile.listFiles()) {

            // if it is a subfolder,e.g Rohan and Ritik,
            // recursiley call function to empty subfolder
            if (subfile.isDirectory()) {

                deleteDirectory(subfile);

            }

            // delete files and empty subfolders
            subfile.delete();
        }

    }

}
