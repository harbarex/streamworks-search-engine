package engine.service;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTCreationException;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import engine.entity.User;
import io.github.cdimascio.dotenv.Dotenv;

public class AuthService {
    final static Logger logger = LogManager.getLogger(AuthService.class);

    static Dotenv dotenv = Dotenv.configure().load();

    private static String secret = dotenv.get("AUTH_SECRET");

    public static String generateToken(User user) throws IllegalArgumentException, UnsupportedEncodingException {
        try {
            Algorithm algorithm = Algorithm.HMAC256(secret);
            String token = JWT.create()
                .withClaim("username", user.getUsername())
                .withClaim("isAdmin", user.isAdmin())
                .sign(algorithm);
            return token;
        } catch(JWTCreationException e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static User validateToken(String token) throws IllegalArgumentException, UnsupportedEncodingException, SQLException {
        try {
            if (token.isEmpty()) {
                return null;
            }
            Algorithm algorithm = Algorithm.HMAC256(secret);
            JWTVerifier verifier = JWT.require(algorithm).build();
            DecodedJWT jwt = verifier.verify(token);
            Claim userClaim = jwt.getClaim("username");
            if (userClaim != null) {
                return UserService.getUser(userClaim.asString());
            }
            return null;
        } catch (JWTVerificationException e) {
            logger.error(e.getMessage());
            return null;
        }
    }
}
