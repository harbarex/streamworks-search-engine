package api.data;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class IndexerMatch {

    private int docId;
    private String url;
    private String title;
    private String context = "";
    private Map<String, Float> features;

    public static String serialize(List<IndexerMatch> matches) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(matches);
    }

    public static List<IndexerMatch> deserialize(String json)
            throws JsonParseException, JsonMappingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        List<IndexerMatch> matches = mapper.readValue(json,
                mapper.getTypeFactory().constructCollectionType(List.class, IndexerMatch.class));
        return matches;
    }

    public IndexerMatch() {
        this.setDocId(0);
        this.setUrl("");
        this.setTitle("");
        this.setContext("");
        this.features = new HashMap<>();
    }

    public IndexerMatch(int docId, String url, String title) {
        this.setDocId(docId);
        this.setUrl(url);
        this.setTitle(title);
        this.setContext("");
        this.features = new HashMap<>();
    }

    public IndexerMatch(int docId, String url, String title, String context) {
        this.setDocId(docId);
        this.setUrl(url);
        this.setTitle(title);
        this.setContext(context);
        this.features = new HashMap<>();
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getDocId() {
        return docId;
    }

    public Map<String, Float> getFeatures() {
        return features;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }

    public void addFeature(String feature, float score) {
        features.put(feature, score);
    }

    public String toString() {
        return "docId: " + docId + ", url: " + url + ", title: " + title + ", features: " + features;
    }
}
