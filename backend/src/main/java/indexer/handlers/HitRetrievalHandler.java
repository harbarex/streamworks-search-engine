package indexer.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import spark.HaltException;
import static spark.Spark.*;
import spark.Request;
import spark.Response;
import spark.Route;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis455.mapreduce.worker.storage.HitStorage;
import edu.upenn.cis455.mapreduce.worker.storage.StorageFactory;
import edu.upenn.cis455.mapreduce.worker.storage.entities.*;

public class HitRetrievalHandler implements Route {

    static Logger logger = LogManager.getLogger(HitRetrievalHandler.class);

    private HashMap<String, HitStorage> hitDBs;

    public HitRetrievalHandler(HashMap<String, HitStorage> hitDBs) {

        this.hitDBs = hitDBs;

    }

    /**
     * Serve hits.
     * POST: /retireve/:hitname
     * Body : WordHitColelctor
     * 
     * 
     * @param req
     * @param resp
     * @return
     * @throws HaltException
     */
    @Override
    public Object handle(Request req, Response resp) throws HaltException {

        String hitGroup = req.params(":hitname");
        String word = req.params(":word");

        logger.debug("(HitRetrievalHandler) get hitname: " + hitGroup + " & word: " + word);

        // make sure this hitGroup is here
        if (!this.hitDBs.containsKey(hitGroup)) {

            halt(400, "Bad Request: This hit group: " + hitGroup + " is not here!");
        }

        try {

            // get from body
            ObjectMapper mapper = new ObjectMapper();

            // get docIDs
            ArrayList<Integer> docIDs = mapper.readValue(req.body(),
                    mapper.getTypeFactory().constructCollectionType(ArrayList.class, Integer.class));

            HashMap<Integer, ArrayList<WordHit>> collectors = new HashMap<Integer, ArrayList<WordHit>>();

            for (int docID : docIDs) {

                ArrayList<WordHit> singleHits = hitDBs.get(hitGroup).getWordHitList(word, docID, "short");
                collectors.put(docID, singleHits);

            }

            String json = mapper.writeValueAsString(collectors);

            // logger.debug("(HitRetrievalHandler) outputs: " + json);

            return json;

        } catch (IOException e) {

            e.printStackTrace();

        }

        halt(500, "JSON parsed error!");

        return "";
    }

}
