package engine.service;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import engine.entity.User;
import engine.utils.PasswordUtils;
import storage.MySQLStorage;

public class UserService {
    private static MySQLStorage storage = null;

    public static void init(MySQLStorage db) throws SQLException {
        storage = db;
        if (getUser("admin") == null) addUser("admin", "admin", true);
    }

    public static void addUser(String username, String password, boolean isAdmin) throws SQLException {
        String hash = PasswordUtils.hashPassword(password);
        PreparedStatement stmt = storage.conn.prepareStatement(
            "INSERT INTO streamwork.user VALUES (?, ?, ?)"
        );
        stmt.setString(1, username);
        stmt.setString(2, hash);
        stmt.setBoolean(3, isAdmin);
        stmt.executeUpdate();
        stmt.close();
    }

    public static User getUser(String username) throws SQLException {
        PreparedStatement stmt = storage.conn.prepareStatement(
            "SELECT * FROM streamwork.user WHERE username = ?"
        );
        stmt.setString(1, username);
        ResultSet resultSet = stmt.executeQuery();
        if (resultSet.next()) {
            User user = new User(username, resultSet.getString("passwordHash"));
            user.setIsAdmin(resultSet.getBoolean("isAdmin"));
            resultSet.close();
            stmt.close();
            return user;
        }
        return null;
    }

    public static boolean isLoginValid(String username, String pwd) throws SQLException {
        User user = getUser(username);
        return user != null && PasswordUtils.isValid(pwd, user.getPasswordHash());
    }
}
