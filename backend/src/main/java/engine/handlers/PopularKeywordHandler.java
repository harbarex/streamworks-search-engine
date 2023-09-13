package engine.handlers;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import engine.entity.PopularKeyword;
import engine.service.KeywordCountService;
import spark.Request;
import spark.Response;
import spark.Route;

public class PopularKeywordHandler implements Route {

    @Override
    public Object handle(Request request, Response response) throws Exception {
        List<PopularKeyword> pop = KeywordCountService.getPopularKeywords(100);
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(pop);
    }
    
}
