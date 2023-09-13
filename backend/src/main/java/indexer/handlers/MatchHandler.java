package indexer.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import spark.HaltException;
import static spark.Spark.*;
import spark.Request;
import spark.Response;
import spark.Route;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.pipeline.*;

import io.github.cdimascio.dotenv.Dotenv;

import edu.upenn.cis455.mapreduce.scheduler.IndexSyncScheduler;
import edu.upenn.cis455.mapreduce.worker.storage.IndexStorage;
import edu.upenn.cis455.mapreduce.worker.storage.HitStorage;
import edu.upenn.cis455.mapreduce.worker.storage.StorageFactory;

import api.data.IndexerMatch;
import indexer.IndexMySQLStorage;
import indexer.nlp.Lemmatizer;
import indexer.scorer.IndexScorer;

public class MatchHandler implements Route {

    static Logger logger = LogManager.getLogger(MatchHandler.class);

    private static Lemmatizer lemmatizer = new Lemmatizer();

    private static Dotenv dotenv = Dotenv.configure().load();
    private String shortLexiconTable = dotenv.get("INDEXER_SHORT_LEXICON");
    private String plainLexiconTable = dotenv.get("INDEXER_PLAIN_LEXICON");
    private String shortDocWordsTable = dotenv.get("INDEXER_SHORT_DOCWORDS");
    private String plainDocWordsTable = dotenv.get("INDEXER_PLAIN_DOCWORDS");
    private String docInfoTable = dotenv.get("INDEXER_DOCINFO");

    // index storage
    private final IndexStorage indexDB;

    // remote storage
    private IndexMySQLStorage remoteDB;

    // scorer for each document
    // scorer contains the way to fetch hits either from local or remote nodes
    private IndexScorer indexScorer;

    public MatchHandler(IndexMySQLStorage remoteDB, IndexStorage indexDB, IndexScorer scorer,
            boolean distributedHits) {

        this.remoteDB = remoteDB;
        this.indexDB = indexDB;
        this.indexScorer = scorer;

    }

    /**
     * Handle the keywords search (API: /indexer/match).
     * General Workflow:
     * 1. Lemmatizer
     * 2. Convert Words to WordIDs
     * => currently, this is used to check whether the word exists in the lexicon
     * 3. Query by WordIDs
     * 4. Assign scores for each matched documents
     * 
     * @param request  : [Request]
     *                 queryParams:
     *                 -query: keywords,
     *                 -count: max n docs,
     *                 -type: doc, pdf
     *                 -location: TODO
     * 
     * @param response
     * @return
     * @throws Exception
     */
    @Override
    public Object handle(Request req, Response resp) throws HaltException {

        // measure execution time
        long handleStartT = System.nanoTime();

        // check query
        String query = req.queryParams("query");

        if (query == null) {

            logger.debug("No query in the incoming request!");

            halt(400, "No query in the request! Please specify the keywords to match in the query!");

        }

        int maxDocCount = (req.queryParams("count") == null) ? -1 : Integer.parseInt(req.queryParams("count"));

        String docType = req.queryParams("type");

        // 1. Lemmatize
        // CoreLabel.word() - the original word, CoreLabel.lemma() - the lemma
        List<CoreLabel> lemmas = lemmatizer.getLemmas(query);

        // 2. Word to WordID
        lemmas = getExistedLemmas(lemmas);

        if (lemmas.size() == 0) {
            // no wordID found
            return "[]";
        }

        // TODO: distributed hits
        // note: matched doc ids are retrieved in matchKeyWords to save time
        ArrayList<Integer> matchedDocIDs = new ArrayList<Integer>();

        // TODO: send parameters to remote server
        List<IndexerMatch> matches = matchKeywords(lemmas, matchedDocIDs, maxDocCount);

        // hitDBs.get(hitGroupID).getWordHitList(word, docID, "short");

        // TODO: handle the returns from remote server
        indexScorer.assignScores(lemmas, matchedDocIDs, matches, lemmas.size());

        // to json string
        try {

            String jsonOutput = IndexerMatch.serialize(matches);

            long handleEndT = System.nanoTime();

            logger.debug("(MatchHandler) execution time : " + ((handleEndT - handleStartT) / 1000000) + " (ms) for "
                    + matches.size() + " (n) results!");

            return jsonOutput;

        } catch (JsonProcessingException e) {

            e.printStackTrace();

            halt(500, "Index server error in the middle of handling json output!");

        }

        return "[]";

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
     * Retrieve the words which are in the lexicon
     * 
     * @param lemmas : [List<CoreLabel>]
     * @return [List<CoreLabel>]
     */
    private List<CoreLabel> getExistedLemmas(List<CoreLabel> lemmas) {

        List<CoreLabel> existedLemmas = new ArrayList<CoreLabel>();

        for (CoreLabel lemma : lemmas) {

            String word = lemma.lemma();

            // Integer wordID = -1;
            for (String variant : getSimpleWordVariants(word)) {

                if (indexDB.hasWord(variant)) {

                    existedLemmas.add(lemma);

                    logger.debug("(MatchHandler) The lemma " + word + " existed in the form of " + variant);

                    break;

                }

            }

        }

        return existedLemmas;

    }

    /**
     * Retrieve the wordID from local index storage.
     * Note: if id not found, that word will not be in the map.
     * 
     * @param lemmas : [List<CoreLabel>]
     * @return [HashMap<CoreLabel, Integer>], lemma to wordID
     */
    private HashMap<CoreLabel, Integer> getWordIDsMap(List<CoreLabel> lemmas) {

        HashMap<CoreLabel, Integer> wordToWordID = new HashMap<CoreLabel, Integer>();

        // boolean noMatchedWordIDs = true;

        for (CoreLabel lemma : lemmas) {

            String word = lemma.lemma();

            // Integer wordID = -1;

            if (indexDB.hasWord(word)) {

                // only retrieve the wordID when it exists in the lexicon
                int wordID = indexDB.getWordID(word);

                // noMatchedWordIDs = false;
                wordToWordID.put(lemma, wordID);

                logger.debug("(MatchHandler) Retrieve wordID for " + word + " : " + wordID);

            }

        }

        return wordToWordID;

    }

    public List<IndexerMatch> matchKeywords(List<CoreLabel> lemmas, ArrayList<Integer> matchedDocIDs,
            int maxDocCount) {

        // construct SQL

        String query = this.makeKeyWordsQuery(lemmas);

        logger.debug("Query for keywords: \r\n" + query);

        try {

            Statement queryStatement = remoteDB.conn.createStatement();

            ResultSet resultSet = queryStatement.executeQuery(query);

            ArrayList<IndexerMatch> matches = new ArrayList<IndexerMatch>();

            int nCollected = 0;

            while (resultSet.next()) {

                // int docId, String url, String title,
                int docID = Integer.parseInt(resultSet.getString("docID"));
                IndexerMatch match = new IndexerMatch(docID,
                        resultSet.getString("url"),
                        resultSet.getString("title"));

                match.addFeature("tfIdf", Float.parseFloat(resultSet.getString("score")));

                // append the match
                matches.add(match);

                // store the matched id here for further retrieving hits from remote nodes
                matchedDocIDs.add(docID);

                nCollected++;

                if ((maxDocCount >= 0) && nCollected == maxDocCount) {

                    // early exit for maxDocCount
                    break;

                }

            }

            return matches;

        } catch (SQLException e) {

            e.printStackTrace();

        }

        return null;

    }

    /**
     * Construct the SQL query based on the lemmas
     * 
     * @param wordIDs : [List<CoreLabel>], a list of lemmas (must exist)
     * 
     * @return
     */
    private String makeKeyWordsQuery(List<CoreLabel> lemmas) {

        // look for first existed wordID (-1 indicates no id for that word)

        String query = this.addWordQuery(0, lemmas.get(0));

        for (int i = 1; i < lemmas.size(); i++) {

            query = this.joinTwoWordQuery(query, i - 1, this.addWordQuery(i, lemmas.get(i)), i);

        }

        // complete the query
        query = this.joinDocInfo(lemmas.size() - 1, query);

        query = query + ";"; // ORDER BY K" + (lemmas.size() - 1) + ".score DESC;";

        return query;
    }

    /**
     * Create a query for a given search word
     * 
     * @param loc   : [int], the index / location of the word in the keyword
     * @param lemma : [String], the lemma created by CoreNLP
     * @return
     */
    private String addWordQuery(int loc, CoreLabel lemma) {

        // loc: the index of the lemma in the keyword
        // which ignore the non-existence keyword in lexicon

        // TODO: check tf-idf scoring equation
        // String query = "(SELECT K" + loc + ".docID, " + "K" + loc + ".ntf * S.idf AS
        // score"
        // + " FROM"
        // + " (SELECT docID, wordID, ntf"
        // + " FROM " + shortDocWordsTable + " WHERE wordID = " + wordID + ") AS K" +
        // loc
        // + " JOIN " + shortLexiconTable + " S ON K" + loc + ".wordID = S.wordID) AS K"
        // + loc;
        String query = "(SELECT K" + loc + ".docID, K" + loc + ".score "
                + " FROM (SELECT D.docID, AVG(D.ntf) * AVG(L.idf) AS score "
                + " FROM " + shortDocWordsTable + " D JOIN " + shortLexiconTable
                + " L ON D.word = L.word WHERE D.word = \"" + lemma.lemma() + "\" "
                + " GROUP BY D.docID) AS K" + loc + ") K" + loc;

        // situation: no wordID in docWord table
        // String query = "(SELECT K" + loc + ".docID, K" + loc + ".score"
        // + " FROM (SELECT D.docID, L.wordID, D.ntf * L.idf AS score"
        // + " FROM " + shortDocWordsTable
        // + " D JOIN " + shortLexiconTable + " L ON D.word = L.word"
        // + " AS K" + loc + " WHERE K" + loc + ".wordID = " + wordID + ") K" + loc;

        return query;

    }

    // /**
    // * Construct the SQL query based on the wordIDs.
    // *
    // * @param wordIDs : [List<Integer>], a list of wordIDs (must exist)
    // *
    // * @return
    // */
    // private String makeKeyWordsQuery(List<Integer> wordIDs) {

    // // look for first existed wordID (-1 indicates no id for that word)

    // String query = this.addWordQuery(0, wordIDs.get(0));

    // for (int i = 1; i < wordIDs.size(); i++) {

    // query = this.joinTwoWordQuery(query, i - 1, this.addWordQuery(i,
    // wordIDs.get(i)), i);

    // }

    // // complete the query
    // query = this.joinDocInfo(wordIDs.size() - 1, query);

    // query = query + " ORDER BY K" + (wordIDs.size() - 1) + ".score DESC;";

    // return query;
    // }

    // /**
    // * Create a query for a given search word
    // *
    // * @param loc : [int], the index / location of the word in the keyword
    // * @param lemma : [String], the lemma created by CoreNLP
    // * @return
    // */
    // private String addWordQuery(int loc, int wordID) {

    // // loc: the index of the lemma in the keyword
    // // which ignore the non-existence keyword in lexicon

    // // TODO: check tf-idf scoring equation
    // // String query = "(SELECT K" + loc + ".docID, " + "K" + loc + ".ntf * S.idf
    // AS
    // // score"
    // // + " FROM"
    // // + " (SELECT docID, wordID, ntf"
    // // + " FROM " + shortDocWordsTable + " WHERE wordID = " + wordID + ") AS K" +
    // // loc
    // // + " JOIN " + shortLexiconTable + " S ON K" + loc + ".wordID = S.wordID) AS
    // K"
    // // + loc;

    // // situation: no wordID in docWord table
    // String query = "(SELECT K" + loc + ".docID, K" + loc + ".score"
    // + " FROM (SELECT D.docID, L.wordID, D.ntf * L.idf AS score"
    // + " FROM " + shortDocWordsTable
    // + " D JOIN " + shortLexiconTable + " L ON D.word = L.word COLLATE
    // utf8mb4_0900_as_cs)"
    // + " AS K" + loc + " WHERE K" + loc + ".wordID = " + wordID + ") K" + loc;

    // return query;

    // }

    /**
     * Join the query of two search words
     * 
     * @param query1 : [String], the query for the previous search word
     * @param q1loc  : [int], the index / location of the search word
     * @param query2 : [String], the query for the current search word
     * @param q2loc  : [int], the index / location of the search word
     * @return [String], the JOIN query for query1 and query2
     */
    private String joinTwoWordQuery(String query1, int q1loc, String query2, int q2loc) {

        String query = "(SELECT K" + q2loc + ".docID, " + "K" + q1loc + ".score + K" + q2loc + ".score AS score"
                + " FROM " + query1 + " JOIN " + query2
                + " ON K" + q1loc + ".docID = K" + q2loc + ".docID) AS K" + q2loc;

        return query;

    }

    /**
     * Prepend query string to
     * join the document query with document info (url & title).
     * 
     * @param lastIndex : [Integer], the last loc of the last word
     * @param docQuery  : [String], the query to get matched documents
     * @return
     */
    private String joinDocInfo(int lastIndex, String docQuery) {

        String infoQuery = "SELECT K" + lastIndex + ".docID, K" + lastIndex + ".score, D.url AS url, D.title AS title"
                + " FROM " + docQuery + " JOIN " + docInfoTable + " D "
                + " ON K" + lastIndex + ".docID = D.docID";

        return infoQuery;

    }

}
