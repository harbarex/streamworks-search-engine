package engine.service;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import storage.MySQLStorage;

public class ConfigService {
    private static MySQLStorage storage;

    public static void init(MySQLStorage db) {
        storage = db;
    }

    public static Map<String, Integer> getConfigs() throws SQLException {
        Statement stmt = storage.conn.createStatement();
        ResultSet resultSet = stmt.executeQuery("SELECT * FROM streamwork.config");
        Map<String, Integer> configs = new HashMap<>();
        
        while (resultSet.next()) {
            configs.put(resultSet.getString("name"), resultSet.getInt("value"));
        }
        resultSet.close();
        stmt.close();
        return configs;
    }

    public static int getConfig(String name) throws SQLException {
        PreparedStatement stmt = storage.conn.prepareStatement("SELECT * FROM streamwork.config WHERE name = ?");
        stmt.setString(1, name);
        ResultSet result = stmt.executeQuery();
        int value = 0;
        if (result.next()) {
            value = result.getInt("value");
        }
        stmt.close();
        return value;
    }

    public static void updateConfig(String name, int value) throws SQLException {
        PreparedStatement stmt = storage.conn.prepareStatement("UPDATE streamwork.config SET value = ? WHERE name = ?");
        stmt.setInt(1, value);
        stmt.setString(2, name);
        stmt.executeUpdate();
        stmt.close();
    }
}
