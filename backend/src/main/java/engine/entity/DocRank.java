package engine.entity;

import java.io.Serializable;

import org.joda.time.Instant;

public class DocRank implements Serializable {
    private float score;
    private int docId;
    private long lastUpdate;

    public DocRank(int docId, float score) {
        this.setScore(score);
        this.setDocId(docId);
        lastUpdate = Instant.now().getMillis();
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public int getDocId() {
        return docId;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }
}
