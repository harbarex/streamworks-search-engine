package engine.service;

import com.sleepycat.collections.StoredSortedMap;

import engine.database.EngineBdbViews;
import engine.entity.DocRank;
import engine.entity.DocRankKey;

public class RankCache {
    private StoredSortedMap<DocRankKey, DocRank> cache;

    public RankCache(EngineBdbViews views) {
        cache = views.getDocRankMap();
    }

    public DocRank getRank(int docId) {
        synchronized(cache) {
            return cache.get(new DocRankKey(docId));
        }
    }

    public void storeRank(int docId, float score) {
        synchronized(cache) {
            cache.put(new DocRankKey(docId), new DocRank(docId, score));
        }
    }
}
