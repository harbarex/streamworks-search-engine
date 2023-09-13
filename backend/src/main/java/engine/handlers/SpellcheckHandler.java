package engine.handlers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import engine.entity.PopularKeyword;
import engine.service.ConfigService;
import engine.service.KeywordCountService;
import spark.Request;
import spark.Response;
import spark.Route;

public class SpellcheckHandler implements Route {

    @Override
    public Object handle(Request request, Response response) throws Exception {
        // extract top number of popular keywords
        // compare with each tokens in the query
        // replace the wrongly pronounced keyword with the popular keyword of least edit distance
        // only replace if the min distance is within some maxDist
        String query = request.queryParams("q");
        int top = ConfigService.getConfig("spellcheck_top");
        int maxDist = ConfigService.getConfig("spellcheck_dist");
        String[] tokens = query.split(" ");
        List<PopularKeyword> popKeywords = KeywordCountService.getPopularKeywords(top);

        List<String> correctTokens = new ArrayList<>();
        boolean hasDifference = false;
        for (String token: tokens) {
            int minDist = maxDist + 1;
            String correctToken = null;
            if (token.trim().isEmpty()) continue;
            for (PopularKeyword popKeyword: popKeywords) {
                int dist = distance(token, popKeyword.getKeyword(), 0, 0);
                if (dist < minDist) {
                    minDist = dist;
                    correctToken = popKeyword.getKeyword();
                }
            }
            if (correctToken != null) {
                correctTokens.add(correctToken);
                hasDifference = true;
            } else {
                correctTokens.add(token);
            }   
        }
        String spellcheck = hasDifference ? String.join(" ", correctTokens) : query;
        response.body(spellcheck);
        return spellcheck;
    }

    private static int distance(String s, String t, int i, int j) {
        return distanceWithMemo(s, t, i, j, new HashMap<>());
    }

    private static int distanceWithMemo(String s, String t, int i, int j, Map<Integer, Map<Integer, Integer>> memo) {
        if (i == s.length()) return t.length() - j;
        if (j == t.length()) return s.length() - i;
        if (!memo.containsKey(i)) {
            memo.put(i, new HashMap<>());
        }
        if (memo.get(i).containsKey(j)) {
            return memo.get(i).get(j);
        }
        int dist;
        if (s.charAt(i) == t.charAt(j)) {
            dist = distanceWithMemo(s, t, i + 1, j + 1, memo);
        } else {
            dist = Math.min(distanceWithMemo(s, t, i + 1, j + 1, memo), distanceWithMemo(s, t, i, j + 1, memo));
            dist = Math.min(dist, distanceWithMemo(s, t, i, j + 1, memo));
            dist++;
        }
        memo.get(i).put(j, dist);        
        return dist;
    }
}
