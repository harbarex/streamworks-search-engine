package engine.filters;

import engine.entity.User;
import spark.Filter;
import spark.Request;
import spark.Response;

public class AdminFilter implements Filter {

    @Override
    public void handle(Request request, Response response) throws Exception {
        if (request.attribute("user") == null || !((User)request.attribute("user")).isAdmin()) {
            response.status(403);
        }
    }
}
