package engine.database;

import java.io.File;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class EngineBdb {
    private static String CLASS_CATALOG = "java_class_catalog";
    private static String DOC_RANK_STORE = "DOC_RANK_STORE";

    private Environment env;
    private StoredClassCatalog javaCatalog;
    private Database docRankDb;

    public EngineBdb(String dir) {
        // set up env
        System.out.println("Opening BDB Environment in: " + dir);
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        env = new Environment(new File(dir), envConfig);
        // set up database
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);

        Database catalogDb = env.openDatabase(null, CLASS_CATALOG, 
                                              dbConfig);

        javaCatalog = new StoredClassCatalog(catalogDb);

        docRankDb = env.openDatabase(null, DOC_RANK_STORE, dbConfig);
    }

    public final StoredClassCatalog getClassCatalog() {
        return javaCatalog;
    } 

    public final Database getDocRankDb() {
        return docRankDb;
    }

    public void close() {
        docRankDb.close();
        javaCatalog.close();
        env.close();
    }
}
