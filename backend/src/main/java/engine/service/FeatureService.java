package engine.service;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import engine.entity.Feature;
import storage.MySQLStorage;


public class FeatureService {
    private static MySQLStorage storage = null;

    public static void init(MySQLStorage db) {
        storage = db;
    }

    public static void addFeature(String name, float coeff, boolean useLog) throws SQLException {
        System.out.println("Saving new feature");
        PreparedStatement stmt = storage.conn.prepareStatement(
            "INSERT INTO streamwork.feature VALUES (?, ?, ?)"
        );
        stmt.setString(1, name);
        stmt.setFloat(2, coeff);
        stmt.setBoolean(3, useLog);
        stmt.executeUpdate();
        stmt.close();
    }

    public static void updateFeature(String name, float coeff, boolean useLog) throws SQLException {
        System.out.println("Editing new feature");
        PreparedStatement stmt = storage.conn.prepareStatement(
            "UPDATE streamwork.feature SET coeff = ?, useLog = ? WHERE name = ?"
        );
        stmt.setFloat(1, coeff);
        stmt.setBoolean(2, useLog);
        stmt.setString(3, name);
        stmt.executeUpdate();
        stmt.close();
    }

    public static List<Feature> getFeatures() throws SQLException {
        List<Feature> features = new ArrayList<>();
        Statement stmt = storage.conn.createStatement();
        ResultSet resultSet = stmt.executeQuery("SELECT * FROM streamwork.feature");
        
        while (resultSet.next()) {
            Feature feat = new Feature(
                resultSet.getString("name"), 
                resultSet.getFloat("coeff"), 
                resultSet.getBoolean("useLog"));
            features.add(feat);
        }
        resultSet.close();
        stmt.close();
        return features;
    }
}
