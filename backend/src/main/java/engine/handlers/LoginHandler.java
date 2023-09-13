package engine.handlers;

import engine.service.AuthService;
import engine.service.UserService;
import spark.Request;
import spark.Response;
import spark.Route;

public class LoginHandler implements Route {

    @Override
    public Object handle(Request request, Response response) throws Exception {
        String username = request.queryParams("username");
        String password = request.queryParams("password");
        
        if (UserService.isLoginValid(username, password)) {
            return AuthService.generateToken(UserService.getUser(username));
        } else {
            response.status(401);
        }
        return "";
    } 
}
