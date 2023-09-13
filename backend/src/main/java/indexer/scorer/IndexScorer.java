package indexer.scorer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import java.net.URL;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.io.IOException;
import java.io.OutputStream;

import javax.swing.plaf.metal.MetalBorders.ScrollPaneBorder;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.pipeline.*;
import io.github.cdimascio.dotenv.Dotenv;

import api.data.IndexerMatch;
import engine.utils.HttpUtils;

import edu.upenn.cis455.mapreduce.worker.storage.IndexStorage;
import edu.upenn.cis455.mapreduce.worker.storage.HitStorage;
import edu.upenn.cis455.mapreduce.worker.storage.StorageFactory;
import edu.upenn.cis455.mapreduce.worker.storage.entities.*;

public class IndexScorer {

    static Logger logger = LogManager.getLogger(IndexScorer.class);

    /**
     * This serves as an instance to score each matched document to the keywords
     */
    private final int numHitShards;
    private final String hitStorageDirectory;
    private final boolean distributedHits;

    private final IndexStorage indexDB;

    private static Dotenv dotenv = Dotenv.configure().load();
    private HashMap<String, ArrayList<String>> remoteHitAddresses = new HashMap<String, ArrayList<String>>();

    private ArrayList<HitStorage> localHitDBs = new ArrayList<HitStorage>();

    private HashMap<String, HashMap<Integer, ArrayList<WordHit>>> remoteHits = new HashMap<String, HashMap<Integer, ArrayList<WordHit>>>();

    public IndexScorer(IndexStorage indexDB, String hitStorageDirectory, boolean distributedHits) {

        this.indexDB = indexDB;

        this.hitStorageDirectory = hitStorageDirectory;
        this.distributedHits = distributedHits;

        this.numHitShards = Integer.parseInt(dotenv.get("INDEXER_NUM_HIT_SHARDS"));

        // TODO: decide whether to load local whatever or not
        if (!this.distributedHits) {

            initLocalHitDBs();

        } else {

            loadRemoteHitConfiguration();

        }

    }

    /**
     * Initiate BerkeleyDB for local hits, naming after hit0, hit1, .... etc.
     */
    private void initLocalHitDBs() {

        for (int i = 0; i < this.numHitShards; i++) {

            localHitDBs.add(StorageFactory.getHitDatabase(this.hitStorageDirectory + "/hit" + i));

        }

    }

    /**
     * Close local hit dbs, used in /shutdown (called by IndexServer)
     */
    public void closeHitDBs() {

        for (HitStorage db : localHitDBs) {

            db.close();

        }

    }

    /**
     * An access method for IndexServer to know the current remote hit addresses.
     * 
     * @return
     */
    public HashMap<String, ArrayList<String>> getRemoteHitAdddresses() {

        return remoteHitAddresses;

    }

    /**
     * Load the remote hit nodes from .env
     */
    private void loadRemoteHitConfiguration() {

        for (int i = 0; i < this.numHitShards; i++) {

            String key = "INDEXER_HIT_SHARD_" + i + "_IPs";

            ArrayList<String> addresses = this.parseIPs(dotenv.get(key));

            remoteHitAddresses.put("hit" + i, addresses);

        }

    }

    /**
     * Parse the ips string from .env
     * (Format: [xxxx.xxx.xxx.xxx:xxx,xxx.xxx.xxx.xxx:xxx,...])
     * 
     * @param ips
     * @return
     */
    private ArrayList<String> parseIPs(String ips) {

        // format [ip1, ip2, ...]
        ArrayList<String> addresses = new ArrayList<String>();

        int start = 1;
        while (start < ips.length()) {

            // find next ","
            int end = ips.indexOf(",", start);

            if (end == -1) {

                // last one
                addresses.add(ips.substring(start, ips.length() - 1));
                start = ips.length();

            } else {

                addresses.add(ips.substring(start, end));

                start = end + 1;

            }

        }

        logger.debug("Get addresses: " + addresses.toString());

        return addresses;

    }

    /**
     * Prepare the wordhits by sending request to the api
     * (/retrieve/:hitname/:word).
     * 
     * @param lemmas
     * @param matchedDocIDs
     */
    private void prepareWordHits(List<CoreLabel> lemmas, ArrayList<Integer> matchedDocIDs) {

        if (!this.distributedHits) {

            return;

        }

        // reset
        remoteHits.clear();

        ObjectMapper mapper = new ObjectMapper();

        try {

            String docIDsJson = mapper.writeValueAsString(matchedDocIDs);

            // loop through the words

            for (CoreLabel lemma : lemmas) {

                String word = lemma.lemma();

                // variants ...
                HashMap<Integer, ArrayList<WordHit>> hits = new HashMap<Integer, ArrayList<WordHit>>();

                for (String variant : getSimpleWordVariants(word)) {

                    if (!indexDB.hasWord(variant)) {

                        continue;

                    }

                    int hitGroupID = getHitGroup(variant);

                    String respJson = this.sendHitRetrievalRequest(hitGroupID, variant, docIDsJson);

                    logger.debug("(IndexScorer) retrieved hits for word " + word + " & variant " + variant + " ... ");

                    if (respJson == null) {

                        continue;

                    }

                    HashMap<Integer, ArrayList<WordHit>> retrievedHits = mapper.readValue(respJson,
                            new TypeReference<HashMap<Integer, ArrayList<WordHit>>>() {
                            });

                    // merge
                    for (Map.Entry<Integer, ArrayList<WordHit>> entry : retrievedHits.entrySet()) {

                        if (!hits.containsKey(entry.getKey())) {

                            hits.put(entry.getKey(), new ArrayList<WordHit>());

                        }

                        hits.get(entry.getKey()).addAll(entry.getValue());

                    }

                }

                remoteHits.put(word, hits);

            }

        } catch (IOException e) {

            e.printStackTrace();

        }

    }

    private List<String> getSimpleWordVariants(String word) {

        HashSet<String> variants = new HashSet<String>();

        variants.add(word);

        // original, first capital, all lower, all capital

        String capitalized = word.substring(0, 1).toUpperCase() + word.substring(1);
        variants.add(capitalized);
        variants.add(word.toLowerCase());
        variants.add(word.toUpperCase());

        return new ArrayList<String>(variants);

    }

    /**
     * Resolve where to retrieve the hit with fault tolerance.
     * 
     * @param hitGroupID : [int]
     * @param json       : [String], the json string of matched docIDs
     * @return
     */
    private String sendHitRetrievalRequest(int hitGroupID, String word, String json) throws IOException {

        // look for the ipaddress that contains the hitGroupID
        ArrayList<String> targetAddresses = this.remoteHitAddresses.get("hit" + hitGroupID);

        for (int i = 0; i < targetAddresses.size(); i++) {

            // always use the first
            // (if the first one is disconnected, pop and append it to the last pos)
            String address = targetAddresses.get(0);

            String dest = "http://" + address + "/retrieve/hit" + hitGroupID + "/" + word;

            // logger.debug("(IndexScorer) Sending request to " + dest + " with json :" +
            // json);
            logger.debug("(IndexScorer) Sending request to " + dest + " ... ");

            try {

                HttpURLConnection conn = this.sendRequest(dest, json);

                if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {

                    String respJson = HttpUtils.readResponse(conn);

                    return respJson;

                }

            } catch (ConnectException e) {

                logger.debug("Get conneciton exeception from " + address + " ...! Try next one");

            }

            // pop out and append it to the last position
            String failedAddress = targetAddresses.remove(0);
            targetAddresses.add(failedAddress);

        }

        // should not have error as the lemma should have hits
        logger.debug("(IndexerScore) unable to find any valid addresses for target word: " + word + " !!!");

        // null (must be not null => for error detection)
        return null;

    }

    /**
     * Send a request for hit retrieval.
     * 
     * @param dest : [String], address with api
     * @param json : [String], json format of a list of document IDs
     * @return
     * @throws IOException
     */
    private HttpURLConnection sendRequest(String dest, String json) throws IOException {

        URL url = new URL(dest);

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");

        conn.setRequestProperty("Content-Type", "application/json");

        OutputStream os = conn.getOutputStream();
        byte[] toSend = json.getBytes();
        os.write(toSend);
        os.flush();

        return conn;

    }

    /**
     * Get the word hit of corresponding word, docID from local or remote nodes.
     * 
     * @return
     */
    private ArrayList<WordHit> getWordHits(int hitGroupID, String word, int docID) {

        if (!this.distributedHits) {

            ArrayList<WordHit> localHits = new ArrayList<WordHit>();

            for (String variant : getSimpleWordVariants(word)) {

                int variantHitGroupID = getHitGroup(variant);

                localHits.addAll(localHitDBs.get(variantHitGroupID).getWordHitList(variant, docID, "short"));

            }

            return localHits;

        } else {

            return this.remoteHits.get(word).get(docID);

        }

    }

    /**
     * Keep a list of weight for each parameter: => total sum to 1
     * (Single keywords)
     * 1. h & a tags: 0.4, 0.4
     * 2. p & span tags: 0.1, 0.1
     * (Multiple keywords) - mimick proximity => total sum to 1
     * 1. All keywords in the same place: 0.6
     * 2. At least two keywords in the same place: 0.3
     * 3. No one at same place: 0.1
     *
     */
    private HashMap<String, Float> weights = new HashMap<String, Float>() {
        {
            // single word
            put("h", (float) 1.3);
            put("a", (float) 1.2);
            // put("s", (float) 1.0);
            put("p", (float) 1.0);

            // multiple words
            put("allIn", (float) 1.5);
            put("twoOrMore", (float) 1.2);
            put("alone", (float) 1.0);

        }
    };

    /**
     * Assign the score to each matched document.
     * Steps:
     * 1. Loop through each document
     * 2. Retrieve the word hits of each document
     * 3. Score
     *
     * @param lemmas        : [List<CoreLabel>], existed lemmas (words)
     * @param matchedDocIDs : [ArrayList<Integer>] matched DocIDs
     * @param matches       : [List<IndexerMatch>], the matched docs and tf-idf
     *                      scores
     * @param numWords      : [int], the original number of keywords (number of
     *                      lemmas,
     *                      including the ones not exist)
     */
    public void assignScores(List<CoreLabel> lemmas, ArrayList<Integer> matchedDocIDs,
            List<IndexerMatch> matches, int numWords) {

        // Note: directly assign the scores to each IndexerMatch instance
        long assignStartT = System.nanoTime();

        // prepare the remote hits if it is in disributed hit mode
        prepareWordHits(lemmas, matchedDocIDs);

        for (int i = 0; i < matches.size(); i++) {

            int docID = matches.get(i).getDocId();

            // look up word hit by lemma,docID
            // k (existed) keywords => k lists of word hits
            HashMap<String, ArrayList<WordHit>> wordHits = new HashMap<String, ArrayList<WordHit>>();

            for (CoreLabel lemma : lemmas) {

                String word = lemma.lemma();

                // wordHits.add(hitDBs.get(""));
                int hitGroupID = getHitGroup(word);

                // logger.debug("(IndexScorer) Get hit group - Word: " + word + " => group: " +
                // hitGroupID);

                ArrayList<WordHit> singleHits = this.getWordHits(hitGroupID, word, docID);

                // ArrayList<WordHit> singleHits = hitDBs.get(hitGroupID).getWordHitList(word,
                // docID, "short");

                wordHits.put(word, singleHits);

                // logger.debug("(IndexScorer) Get single hits for word " + word + " , docID " +
                // docID);
                // logger.debug("(IndexScorer) --- " + singleHits.toString());

            }

            // evaluate
            evaluateAndGetContext(matches.get(i), lemmas, wordHits, numWords);

        }

        long assignEndT = System.nanoTime();

        logger.debug("(IndexScorer) execution time : " + ((assignEndT - assignStartT) / 1000000) + " (ms)");

    }

    /**
     * Evaluate the document & get the context from the document for the keyword.
     * Note: directly update in the match object.
     * 
     * @param match
     * @param lemmas   : [List<CoreLabel>],
     *                 map.size(): number of keywords
     * @param wordHits : [HashMap],
     *                 wordHits.size(): number of keywords,
     *                 wordHits.get(i).size(): number of hits of the word i in this
     *                 document.
     *                 each hit: (docID, -1, lemma, tag, pos, cap, 12, context)
     */
    private void evaluateAndGetContext(IndexerMatch match, List<CoreLabel> lemmas,
            HashMap<String, ArrayList<WordHit>> wordHits, int numWords) {

        // ratio: number of matched words / number of key words
        float kwRatio = (float) lemmas.size() / numWords;

        // Part 1 & 2 => type score : only based on type of tag
        int nTotalTags = 0;

        // tag - hit pair
        HashMap<String, Integer> tagCnts = new HashMap<String, Integer>() {
            {
                // these four are used for general tags
                put("a", 0);
                put("h", 0);
                // put("s", 0);
                put("p", 0);
            }
        };

        // unique tag : [pos, pos, ...]
        HashMap<String, ArrayList<Integer>> proximityCnts = new HashMap<String, ArrayList<Integer>>();

        // unique tag : [context, context, context ...]
        // corresponding to each pos
        HashMap<String, ArrayList<String>> plainContexts = new HashMap<String, ArrayList<String>>();
        HashMap<String, ArrayList<String>> titleContexts = new HashMap<String, ArrayList<String>>();

        // collect tag counts
        for (ArrayList<WordHit> singleHits : wordHits.values()) {

            for (WordHit h : singleHits) {

                String tname = h.getTag();

                String tgroup = tname.substring(0, 1);

                tagCnts.put(tgroup, tagCnts.get(tgroup) + 1);

                if (!proximityCnts.containsKey(tname)) {

                    proximityCnts.put(tname, new ArrayList<Integer>());

                    if (tname.startsWith("h")) {

                        titleContexts.put(tname, new ArrayList<String>());

                    } else if (tname.startsWith("p")) {

                        plainContexts.put(tname, new ArrayList<String>());

                    }

                }

                proximityCnts.get(tname).add(h.getPos());

                if (tname.startsWith("h")) {

                    titleContexts.get(tname).add(h.getContext());

                } else if (tname.startsWith("p")) {

                    plainContexts.get(tname).add(h.getContext());

                }

                nTotalTags++;

            }

        }

        float tagScore = assignTagScore(tagCnts, nTotalTags);

        // assign tag score (note: multiply with (n existed words) / (n key words))
        match.addFeature("tagScore", tagScore * kwRatio);
        // match.setTagScore(tagScore * kwRatio);

        // Part 2 => proximity score :
        // 1. based on tag overlap
        // 2. if have the same tag => consider position in the tag
        // Note: no effect if it is one keyword search
        if (numWords == 1) {

            // match.setProximityScore(1);
            match.addFeature("proximityScore", 1);

        } else {

            float proximityScore = assignProximityScore(proximityCnts, lemmas.size());

            // match.setProximityScore(proximityScore * kwRatio);
            match.addFeature("proximityScore", proximityScore * kwRatio);
        }

        // Part 3 => fetch context:
        // retrieve the overlapped <p> or <span> ...
        String matchedContext = assignContext(titleContexts, plainContexts, lemmas);
        // new ArrayList<CoreLabel>(lemmaMap.keySet()));

        // logger.debug("(IndexScorer) Get context : " + matchedContext);

        match.setContext(matchedContext);

        // Part 4 => adjust the score
        // match.setIndexerScore(match.getTfIdf() + (match.getTagScore() *
        // match.getProximityScore()));

    }

    /**
     * Retrieve the group id of the word.
     * Note: this has to be the same with the FieldGrouping in Stormlite.
     * Number of shards is based on number of hitDBs.
     * 
     * @param word : [String], the lemma
     * @return [Integer], the hit group (e.g. hit0, hit1, ....)
     */
    public int getHitGroup(String word) {

        int hash = 0;

        hash ^= word.hashCode();

        hash = hash % this.numHitShards;

        if (hash < 0)
            hash = hash + this.numHitShards;

        return hash;

    }

    /**
     * Basically, sum(tag weight * count) / nTotalTags
     * 
     * @param tagCounts
     * @param nTotalTags
     * @return
     */
    private float assignTagScore(HashMap<String, Integer> tagCounts, int nTotalTags) {

        float score = 0;

        for (String tag : tagCounts.keySet()) {

            score += weights.get(tag) * tagCounts.get(tag);

        }

        if (nTotalTags > 0) {

            score = score / nTotalTags;

        } else {

            score = 1;

        }

        return score;

    }

    /**
     * Basically, consider overlap at first, then check the position values inside
     * the tag.
     * Conditions: allIn, twoOrMore, alone
     * TODO: look into the positions!
     * 
     * @return
     */
    private float assignProximityScore(HashMap<String, ArrayList<Integer>> proximityCounts, int numExistedWords) {

        float score = 0;

        for (String uniqueTag : proximityCounts.keySet()) {

            float tagW = weights.get(uniqueTag.substring(0, 1));

            int nInTag = proximityCounts.get(uniqueTag).size();

            if (nInTag == numExistedWords && numExistedWords >= 2) {

                // use allIn
                score += tagW * weights.get("allIn");

                // TODO: look into pos

            }

            else if (nInTag >= 2) {

                score += tagW * weights.get("twoOrMore");

            } else {

                score += tagW * weights.get("alone");

            }

        }

        if (proximityCounts.size() > 0) {

            // normalize with the number of unique tags
            score = score / proximityCounts.size();

        } else {

            score = 1;

        }

        return score;

    }

    /**
     * Choose the one
     * <p>
     * or <s> that has the most words.
     * 
     * @param tagContexts
     * @param lemmas
     * @return
     */
    private String assignContext(HashMap<String, ArrayList<String>> titleContexts,
            HashMap<String, ArrayList<String>> plainContexts, List<CoreLabel> lemmas) {

        String context = "";

        String maxTag = "";
        int maxOverlap = 0;

        for (String uniqueTag : plainContexts.keySet()) {

            // // ignore h & a
            // if (uniqueTag.startsWith("h") || uniqueTag.startsWith("a")) {
            // // if (uniqueTag.startsWith("h1") || uniqueTag.startsWith("a")) {

            // continue;

            // }

            if (plainContexts.get(uniqueTag).size() == maxOverlap) {

                // check the tag is close to the beginning of the page or not
                int previous = Integer.parseInt(maxTag.substring(1));
                int curr = Integer.parseInt(uniqueTag.substring(1));

                if (curr < previous) {

                    // change to curr
                    maxTag = uniqueTag;
                    context = plainContexts.get(uniqueTag).get(0);

                }

            }

            else if (plainContexts.get(uniqueTag).size() > maxOverlap) {

                context = plainContexts.get(uniqueTag).get(0);
                maxTag = uniqueTag;

            }

        }

        if (context.equals("")) {

            for (String uniqueTag : titleContexts.keySet()) {

                if (!uniqueTag.startsWith("h1") && titleContexts.get(uniqueTag).size() > 0) {

                    context = titleContexts.get(uniqueTag).get(0);

                }

            }

        }

        // take h1
        if (context.equals("")) {

            for (String uniqueTag : titleContexts.keySet()) {

                if (titleContexts.get(uniqueTag).size() > 0) {

                    context = titleContexts.get(uniqueTag).get(0);

                }

            }

        }

        // conver to lower case to find position
        String lowerC = context.toLowerCase();
        String boldedContext = context;

        // find corresponding words
        List<String> realWords = new ArrayList<String>();

        for (CoreLabel lemma : lemmas) {

            String word = lemma.lemma().toLowerCase();

            if (lowerC.contains(word)) {

                realWords.add(context.substring(lowerC.indexOf(word), lowerC.indexOf(word) + word.length()));

            }

        }

        for (String wd : realWords) {

            boldedContext = boldedContext.replace(wd, "<b>" + wd + "</b>");

        }

        // add ...
        boldedContext = boldedContext + " ... ";
        // logger.debug("Get context: " + boldedContext);
        return boldedContext;

    }

    /**
     * This one implements a simple type scoring system
     *
     * @param counts
     * @param nWords
     * @return
     */
    private float evaluateTypeScore(HashMap<String, Integer> counts, int nWords) {

        int nTotalTags = 0;

        float singleScore = 0;
        float multipleScore = 0;

        for (String tag : counts.keySet()) {

            nTotalTags += counts.get(tag);

            // title
            if (tag.startsWith("h")) {

                // logger.debug("h: " + counts.get(tag));
                singleScore += (weights.get("h") * counts.get(tag));

            }

            // anchor
            else if (tag.startsWith("a")) {

                // logger.debug("a: " + counts.get(tag));
                singleScore += (weights.get("a") * counts.get(tag));

            }

            // others: should start with p or span
            else {

                // logger.debug("p or span: " + counts.get(tag));

                singleScore += (weights.get("p") * counts.get(tag));

            }

            // multiple
            if (counts.get(tag) == nWords) {

                // depending on type?
                // logger.debug("All in " + tag);
                multipleScore += weights.get("all");

            }

            else if (counts.get(tag) >= 2) {

                // logger.debug("Two or more " + tag);
                multipleScore += weights.get("twoOrMore");

            }

            else {

                // logger.debug("Alone: " + tag);
                multipleScore += weights.get("alone");

            }

        }

        singleScore = (nTotalTags == 0) ? singleScore : singleScore / nTotalTags;

        multipleScore = (counts.size() == 0) ? multipleScore
                : multipleScore /
                        counts.size();

        logger.debug("Single score: " + singleScore);
        logger.debug("Multiple score: " + multipleScore);
        return singleScore * multipleScore;

    }

    // /**
    // * Assign the score to each matched document.
    // * Steps:
    // *
    // *
    // * @param lemmas
    // * @param matches
    // */
    // public void assignScores(HashMap<CoreLabel, Integer> lemmaMap,
    // List<IndexerMatch> matches) {

    // ArrayList<Integer> wordIDs = this.getWordIDs(lemmas);

    // HashMap<Integer, IndexerMatch> docIDs = new HashMap<Integer, IndexerMatch>();

    // for (int i = 0; i < matches.size(); i++) {

    // // add more
    // // docIDs.add(matches.get(i).getDocId());
    // docIDs.put(matches.get(i).getDocId(), matches.get(i));

    // if ((docIDs.size() == batchSize) || (i == matches.size() - 1)) {

    // // batch process
    // ArrayList<HashMap<Integer, HashMap<String, ArrayList<WordHit>>>> lemmaHits =
    // new ArrayList<HashMap<Integer, HashMap<String, ArrayList<WordHit>>>>();

    // for (int wordID : wordIDs) {

    // logger.debug("Get id: " + wordID);

    // if (wordID == -1) {

    // // lemmaHits.add(null);
    // continue;

    // }

    // lemmaHits.add(this.indexDB.getWordDocHits(wordID, new
    // ArrayList<Integer>(docIDs.keySet()), true));

    // }

    // // evaluate & assign score to each document
    // for (int docID : docIDs.keySet()) {

    // // collection (counter) for multiple words
    // HashMap<String, Integer> docTagCounts = new HashMap<String, Integer>();

    // for (int j = 0; j < lemmaHits.size(); j++) {

    // // logger.debug("docID: " + docID + " from " + lemmaHits.get(j));
    // if (lemmaHits.get(j).get(docID) != null) {

    // for (String tag : lemmaHits.get(j).get(docID).keySet()) {

    // docTagCounts.put(tag, 1 + docTagCounts.getOrDefault(tag, 0));

    // }

    // }

    // }

    // float typeScore = this.evaluateTypeScore(docTagCounts, lemmas.size());

    // // update (sum up) the score
    // docIDs.get(docID).setTypeScore(typeScore);
    // docIDs.get(docID).updateIndexScore();
    // logger.debug(docIDs.get(docID).toString());

    // // retrieve the best <p> <span>

    // }

    // // clean visited docIDs
    // docIDs.clear();

    // }

    // }

    // }

}
