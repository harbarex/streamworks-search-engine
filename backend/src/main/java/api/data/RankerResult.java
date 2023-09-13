package api.data;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RankerResult {
    private int docid;
    private float score;

    public static String serialize(List<RankerResult> ranks) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(ranks);
    }

    public static List<RankerResult> deserialize(String serRanks) throws JsonParseException, JsonMappingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        List<RankerResult> results = mapper.readValue(serRanks,
                mapper.getTypeFactory().constructCollectionType(List.class, RankerResult.class));
        return results;
    }

    public RankerResult() {
        this.docid = -1;
        this.score = 0;
    }

    public RankerResult(int docid, float score) {
        this.docid = docid;
        this.score = score;
    }

    public int getDocid() {
        return docid;
    }
    public float getScore() {
        return score;
    }
    public void setScore(float score) {
        this.score = score;
    }
    
    public void setDocid(int docid) {
        this.docid = docid;
    }

    public String toString() {
        return "RankerResult(docid = " + docid + ", score = " + score;
    }
}
