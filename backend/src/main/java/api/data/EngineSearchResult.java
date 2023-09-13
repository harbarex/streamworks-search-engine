package api.data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EngineSearchResult {
    private String url;
    private String type;
    private String title;
    private String displayText;
    private float totalScore;
    private Map<String, Float> features;

    public static String toJson(List<EngineSearchResult> results) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(results);
    }

    public EngineSearchResult(String url, String type, String title, String displayText, float score) {
        this.setUrl(url);
        this.setType(type);
        this.setTitle(title);
        this.setDisplayText(displayText);
        totalScore = score;
        features = new HashMap<>();
    }

    public float getTotalScore() {
        return totalScore;
    }

    public void setTotalScore(float totalScore) {
        this.totalScore = totalScore;
    }

    public Map<String, Float> getFeatures() {
        return features;
    }

    public void addFeature(String key, float value) {
        features.put(key, value);
    }

    public String getDisplayText() {
        return displayText;
    }

    public void setDisplayText(String displayText) {
        this.displayText = displayText;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
    
}