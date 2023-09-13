package indexer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static spark.Spark.*;

import java.io.FileNotFoundException;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.github.cdimascio.dotenv.Dotenv;

import edu.upenn.cis455.mapreduce.worker.storage.IndexStorage;
import edu.upenn.cis455.mapreduce.worker.storage.StorageFactory;

import indexer.handlers.MatchHandler;
import indexer.handlers.IndexHitMonitorHandler;
import indexer.scorer.IndexScorer;

import storage.MySQLConfig;

public class IndexServer {

    static Logger logger = LogManager.getLogger(IndexServer.class);

    // TOOD: set up IndexStorage paths

    // TODO: update Remote DB Configuration
    private boolean shouldDown = false;

    private final int port;

    // index & hit storage
    private final String indexStorageDirectory;
    private final IndexStorage indexDB;

    private final String hitStorageDirectory;

    private boolean distributedHits = true;

    // remote storage
    private IndexMySQLStorage remoteDB;

    // scorer for match handler
    private IndexScorer indexScorer;

    public IndexServer(int port, String indexStorageDirectory, String hitStorageDirectory, boolean distributedHits) {

        this.port = port;

        this.indexStorageDirectory = indexStorageDirectory;
        this.hitStorageDirectory = hitStorageDirectory;
        this.distributedHits = distributedHits;

        // DBs init
        indexDB = StorageFactory.getIndexDatabase(this.indexStorageDirectory);

        // remote DB
        Dotenv dotenv = Dotenv.configure().load();
        MySQLConfig conf = new MySQLConfig(
                dotenv.get("INDEXER_MYSQL_DBNAME"),
                dotenv.get("INDEXER_MYSQL_USERNAME"),
                dotenv.get("INDEXER_MYSQL_PASSWORD"),
                dotenv.get("INDEXER_MYSQL_HOSTNAME"),
                dotenv.get("INDEXER_MYSQL_PORT"));
        remoteDB = new IndexMySQLStorage(conf);

        // init scorer for hit storage
        this.indexScorer = new IndexScorer(this.indexDB, this.hitStorageDirectory, this.distributedHits);

        // Set up server configuration with spark
        port(this.port);

        threadPool(10);

        // Set up monitor page
        get("/monitor", new IndexHitMonitorHandler(this.indexScorer));
        get("/", new IndexHitMonitorHandler(this.indexScorer));

        // Set up APIs
        get("/indexer/match", new MatchHandler(remoteDB, indexDB, this.indexScorer, distributedHits));

        // TODO: implement shutdown if needed
        get("/shutdown", (req, res) -> {

            this.shouldDown = true;

            // join dummy thread
            try {

                connectionKeeperT.join(1000);

            } catch (InterruptedException e) {

                e.printStackTrace();

            }

            // remote
            remoteDB.close();

            // indexDB
            indexDB.close();

            // local hit storage if exist
            if (!this.distributedHits) {

                this.indexScorer.closeHitDBs();

            }

            // stop server
            stop();

            return "Gracefully shutdown!";

        });
        // e.g. close Berkeley DBs & remote connections

        // start connection keeper thread
        connectionKeeperT.start();

        System.out.println("Index Server started on port: " + this.port);

        awaitInitialization();

    }

    // TODO: set up a daemon thread to keep connected
    Runnable connectionKeeper = new Runnable() {

        @Override
        public void run() {

            // send every 10 seconds
            while (!shouldDown) {

                try {

                    // send dummy query to remote server
                    if (remoteDB != null) {

                        remoteDB.sendDummyQuery();

                        logger.debug(
                                "(IndexServer) Successfully send the dummy query ... trying to keep connected ...");

                    }

                    // sleep 30 s
                    Thread.sleep(30000);

                } catch (InterruptedException e) {

                    e.printStackTrace();
                }

            }

        }

    };

    public static void createDirectory(String directory) {

        if (!Files.exists(Paths.get(directory)))

        {

            try {

                logger.warn(
                        "(IndexServer) The specified directory : " + directory
                                + " does not exist! Will create one! Note: this means the index server may have no data to search");

                Files.createDirectories(Paths.get(directory));

            } catch (IOException e) {

                e.printStackTrace();

            }
        }

    }

    Thread connectionKeeperT = new Thread(connectionKeeper);

    public static void main(String[] args) throws Exception {

        org.apache.logging.log4j.core.config.Configurator.setLevel("indexer", Level.DEBUG);

        if (args.length != 4) {

            System.out.println(
                    "Usage: please provide 4 arguments: [port] [index storage path] [enable distributed hits mode]");

        }

        int port = Integer.parseInt(args[0]);
        String indexStorageDirectory = args[1];

        String hitStorageDirectory = args[2];
        Boolean enabledDistributedHitsMode = Boolean.parseBoolean(args[3]);

        // check and create directory if needed
        createDirectory(indexStorageDirectory);
        createDirectory(hitStorageDirectory);

        // instantiate server
        IndexServer indexer = new IndexServer(port, indexStorageDirectory, hitStorageDirectory,
                enabledDistributedHitsMode);

    }
}
