package indexer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static spark.Spark.*;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis455.mapreduce.worker.storage.HitStorage;
import edu.upenn.cis455.mapreduce.worker.storage.StorageFactory;

import indexer.handlers.HitRetrievalHandler;

public class HitServer {

    static Logger logger = LogManager.getLogger(HitServer.class);

    /**
     * This server is used to serve the WordHits in each worker (node)
     * Main api:
     * GET : (/retrieve/hits?word=xxxx&docIDs=[xx,xx,xx,...]&hitGroup=xx)
     * GET : (/retrieve/hitsrange)
     * => to tell the master what is the range of hits it serves
     */
    private final int port;
    private final String hitStorageDirectory;

    // hashmap key: path (i.e. hitx), value: hitDB
    private HashMap<String, HitStorage> hitDBs = new HashMap<String, HitStorage>();

    /**
     * Traverse the hitStorageDirectory to know which hits can be served
     * 
     * @return : [Set<String>] e.g. {hit0, hit1, ...}
     */
    private Set<String> getHitShards() {

        HashSet<String> shards = new HashSet<String>();

        File[] files = new File(this.hitStorageDirectory).listFiles();

        for (File f : files) {

            // Note: f.toString() => full path name
            String fn = f.toString();
            fn = fn.replace(hitStorageDirectory, "");

            // hit folder must be named after hitX
            if (fn.startsWith("/hit")) {

                shards.add(fn.substring(1));

            }

        }

        return shards;

    }

    public HitServer(int port, String hitStorageDirectory) {

        this.port = port;

        if (hitStorageDirectory.endsWith("/")) {

            // remove trailing slash
            hitStorageDirectory = hitStorageDirectory.substring(0, hitStorageDirectory.length() - 1);

        }

        this.hitStorageDirectory = hitStorageDirectory;

        // instantiate BerkeleyDB
        Set<String> shards = this.getHitShards();

        for (String filename : shards) {

            this.hitDBs.put(filename, StorageFactory.getHitDatabase(this.hitStorageDirectory + "/" + filename));

        }

        logger.debug("(HitNode) Initializing ... ! Serving : " + this.hitDBs.keySet().toString());

        // Set up server configuration with spark
        port(this.port);

        threadPool(10);

        get("/access/:hitname", (req, res) -> {

            String hitGroup = req.params(":hitname");

            if (hitGroup != null && hitDBs.containsKey(hitGroup)) {

                return "1";

            }

            halt(404, "Hit group not found here!");

            return "";

        });

        get("/retrieve/hitsrange", (req, res) -> {

            Set<String> servedHits = hitDBs.keySet();

            try {

                ObjectMapper mapper = new ObjectMapper();

                String json = mapper.writeValueAsString(servedHits);

                logger.debug("(HitNode) sending message: " + json + " ... ");

                return json;

            } catch (JsonProcessingException e) {

                e.printStackTrace();

            }

            // send 404
            halt(404, "No hits served!");

            return "";

        });

        post("/retrieve/:hitname/:word", new HitRetrievalHandler(hitDBs));

        System.out.println("Index Server started on port: " + this.port);

        awaitInitialization();

    }

    public static void main(String[] args) {

        // port, folder to serve
        org.apache.logging.log4j.core.config.Configurator.setLevel("indexer", Level.DEBUG);

        if (args.length != 2) {

            System.out.println(
                    "Usage: please provide 2 arguments: [port] [hit storage path]");

        }

        HitServer server = new HitServer(Integer.parseInt(args[0]), args[1]);

    }

}
