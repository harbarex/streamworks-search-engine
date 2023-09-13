package engine.handlers;

import engine.service.AuthService;
import engine.service.UserService;
import spark.Request;
import spark.Response;
import spark.Route;

public class RegisterHandler implements Route {

    @Override
    public Object handle(Request request, Response response) throws Exception {
        String username = request.queryParams("username");
        String password = request.queryParams("password");

        if (UserService.getUser(username) != null) {
            response.status(409);
        } else {
            UserService.addUser(username, password, false);
            String token = AuthService.generateToken(UserService.getUser(username));
            return token;
        }

        return "";
    }
}
