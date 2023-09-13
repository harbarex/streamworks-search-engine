package engine.entity;

import java.io.Serializable;

public class DocRankKey implements Serializable {
    private int docId;

    public DocRankKey(int docId) {
        this.docId = docId;
    }

    public int getDocId() {
        return docId;
    }
}
