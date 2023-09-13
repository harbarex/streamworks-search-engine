package engine.database;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.collections.StoredSortedMap;

import engine.entity.DocRank;
import engine.entity.DocRankKey;

public class EngineBdbViews {
    private StoredSortedMap<DocRankKey, DocRank> docRankMap;

    public EngineBdbViews(EngineBdb db) {
        ClassCatalog catalog = db.getClassCatalog();
        EntryBinding<DocRankKey> docRankKeyBinding = new SerialBinding<>(catalog, DocRankKey.class);
        EntryBinding<DocRank> docRankBinding = new SerialBinding<>(catalog, DocRank.class);
        docRankMap = new StoredSortedMap<>(db.getDocRankDb(), docRankKeyBinding, docRankBinding, true);
    }

    public void clearCache() {
        synchronized(docRankMap) {
            docRankMap.clear();
        }
    }

    public StoredSortedMap<DocRankKey, DocRank> getDocRankMap() {
        return docRankMap;
    }
}
