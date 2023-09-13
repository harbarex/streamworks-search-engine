package engine.filters;

import engine.entity.User;
import engine.service.AuthService;
import spark.Filter;
import spark.Request;
import spark.Response;

public class TokenFilter implements Filter {

    @Override
    public void handle(Request request, Response response) throws Exception {
        if (request.pathInfo().equals("/api/login") || request.pathInfo().equals("/api/register")) {
            return;
        }
        String authHeader = request.headers("Authorization");
        if (authHeader != null) {
            String[] parts = authHeader.split(" ");
            if (parts.length == 2 && parts[0].equals("Bearer")) {
                String accessToken = parts[1];
                User user = AuthService.validateToken(accessToken);
                request.attribute("user", user);
            }
        }
    }
}
