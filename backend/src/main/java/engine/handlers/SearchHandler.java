package engine.handlers;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.Instant;

import api.data.EngineSearchResult;
import api.data.IndexerMatch;
import api.data.RankerResult;
import engine.database.EngineBdbViews;
import engine.entity.DocRank;
import engine.entity.Feature;
import engine.service.FeatureService;
import engine.service.KeywordCountService;
import engine.service.PerformanceService;
import engine.service.RankCache;
import engine.utils.HttpUtils;
import spark.Request;
import spark.Response;
import spark.Route;

public class SearchHandler implements Route {
    private static String SEARCH_TOTAL = "search_total";
    private static String SEARCH_INDEXER = "search_indexer";
    private static String SEARCH_RANKER = "search_ranker";

    class CountKeywordThread extends Thread {
        private String query;
    
        public CountKeywordThread(String query) {
            this.query = query;
        }
    
        @Override
        public void run() {
            String[] tokens = query.split(" ");
            for (String token: tokens) {
                if (token.isBlank()) continue;
                try {
                    List<IndexerMatch> indexerMatches = getIndexerMatches(token.toLowerCase(), "doc");
                    if (!indexerMatches.isEmpty()) {
                        KeywordCountService.incCount(token.toLowerCase());
                    }
                } catch (IOException | SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    final static Logger logger = LogManager.getLogger(SearchHandler.class);
    private List<String> indexers;
    private List<String> rankers;
    private int nextIndexer;
    private int nextRanker;
    private RankCache rankCache;

    public SearchHandler(List<String> indexers, List<String> rankers, EngineBdbViews views) {
        this.indexers = indexers;
        this.rankers = rankers;
        nextIndexer = 0;
        nextRanker = 0;
        rankCache = new RankCache(views);
    }

    @Override
    public Object handle(Request request, Response response) throws Exception {
        long searchStart = System.currentTimeMillis();
        String searchQuery = request.queryParams("q");
        String searchType = request.queryParams("type");

        // update popular keyword
        CountKeywordThread t = new CountKeywordThread(searchQuery);
        t.setDaemon(true);
        t.start();

        // ask indexer which documents match the query
        long indexerStart = System.currentTimeMillis();
        List<IndexerMatch> indexerMatches = new ArrayList<>();
        try {
            indexerMatches = getIndexerMatches(searchQuery, searchType);
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
        PerformanceService.saveMetrics(SEARCH_INDEXER, "latency", System.currentTimeMillis() - indexerStart, System.currentTimeMillis());
        
        // extract the document with rank score in cache
        long rankStart = System.currentTimeMillis();
        List<DocRank> docsWithRank = new ArrayList<>();
        Set<Integer> matcheIdsWithoutRank = new HashSet<>();

        Map<Integer, IndexerMatch> indexerMap = new HashMap<>();

        if (indexerMatches != null) {
            for (IndexerMatch match: indexerMatches) {
                int docId = match.getDocId();
                DocRank cachedRank = rankCache.getRank(docId);
                indexerMap.put(docId, match);
                if (cachedRank != null && Instant.now().getMillis() - cachedRank.getLastUpdate() < 5 * 60 * 1000) {  // TODO: get timeout from database
                    docsWithRank.add(cachedRank);
                } else {
                    matcheIdsWithoutRank.add(match.getDocId());
                }
            }
        }

        // ask rankers the score of docs
        if (!matcheIdsWithoutRank.isEmpty()) {
            List<RankerResult> rankerResults = getRankerResults(matcheIdsWithoutRank);
            for (RankerResult rankerResult: rankerResults) {
                rankCache.storeRank(rankerResult.getDocid(), rankerResult.getScore());
                docsWithRank.add(new DocRank(rankerResult.getDocid(), rankerResult.getScore()));
            }
        }
        PerformanceService.saveMetrics(SEARCH_RANKER, "latency", System.currentTimeMillis() - rankStart, System.currentTimeMillis());

        // calculate final score from indexer and ranker scores
        List<EngineSearchResult> results = new ArrayList<>();
        List<Feature> featureCoeffs = FeatureService.getFeatures();
        for (DocRank docRank: docsWithRank) {
            int docId = docRank.getDocId();
            IndexerMatch indexerMatch = indexerMap.get(docId);
            EngineSearchResult result = new EngineSearchResult(indexerMatch.getUrl(), searchType, 
                indexerMatch.getTitle(), indexerMatch.getContext(), 0);
            for (String feat: indexerMatch.getFeatures().keySet()) {
                float featScore = indexerMatch.getFeatures().get(feat);
                result.addFeature(feat, featScore);
            }
            result.addFeature("rankerScore", docRank.getScore());
            calculateTotalScore(result, featureCoeffs);
            results.add(result);
        }
        results.sort((EngineSearchResult result1, EngineSearchResult result2) -> Float.compare(result2.getTotalScore(), result1.getTotalScore()));

        response.type("application/json");
        String body = EngineSearchResult.toJson(results);
        response.body(body);
        
        PerformanceService.saveMetrics(SEARCH_TOTAL, "latency", System.currentTimeMillis() - searchStart, System.currentTimeMillis());

        return body;
    }

    private void calculateTotalScore(EngineSearchResult result, List<Feature> featureCoeffs) {
        float totalScore = 0;
        for (Map.Entry<String, Float> feat: result.getFeatures().entrySet()) {
            for (Feature featCoeff: featureCoeffs) {
                if (featCoeff.getName().equals(feat.getKey())) {
                    float featScore = feat.getValue();
                    if (featCoeff.isUseLog()) {
                        featScore = (float) Math.log(featScore);
                    }
                    totalScore += featScore * featCoeff.getCoeff();
                }
            }
        }
        result.setTotalScore(totalScore);
    }

    private String getNextIndexer() {
        synchronized(indexers) {
            String indexerAddress = indexers.get(nextIndexer);
            nextIndexer = (nextIndexer + 1) % indexers.size();
            return indexerAddress;
        }
    }

    private List<IndexerMatch> getIndexerMatches(String query, String type) throws IOException {
        List<IndexerMatch> results = new ArrayList<>();
        String indexer = getNextIndexer();
        String formattedQuery = query.replace(" ", "%20");
        HttpURLConnection conn = HttpUtils.sendRequest(indexer, "GET", "indexer/match?count=100&type=" + type +"&query=" + formattedQuery, null);
        if (conn.getResponseCode() != 200) {
            logger.error("Indexer returns error: " + conn.getResponseMessage());
        } else {
            String indexerResp = HttpUtils.readResponse(conn);
            results = IndexerMatch.deserialize(indexerResp);
        }
        conn.disconnect();
        return results;
    }

    private String getNextRanker() {
        synchronized(rankers) {
            String rankerAddress = rankers.get(nextRanker);
            nextRanker = (nextRanker + 1) % rankers.size();
            return rankerAddress;
        }
    }

    private List<RankerResult> getRankerResults(Set<Integer> docids) throws IOException {
        List<RankerResult> results = new ArrayList<>();
        String ranker = getNextRanker();
        HttpURLConnection conn = HttpUtils.sendRequest(ranker, "POST", "ranker/rank", docids.toString());
        if (conn.getResponseCode() != 200) {
            logger.error("Ranker returns error: " + conn.getResponseMessage());
        } else {
            String rankerResp = HttpUtils.readResponse(conn);
            results = RankerResult.deserialize(rankerResp);
        }
        conn.disconnect();

        return results;
    }
}
