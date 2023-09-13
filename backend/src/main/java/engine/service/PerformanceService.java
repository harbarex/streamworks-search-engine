package engine.service;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import storage.MySQLStorage;

public class PerformanceService {

    private static MySQLStorage storage = null;
    private static ExecutorService executor;

    public static void init(MySQLStorage db) {
        storage = db;
        executor = Executors.newFixedThreadPool(10);
    }

    public static void saveMetrics(String name, String metrics, float value, long ts) {
        executor.execute(() -> {
            PreparedStatement stmt;
            try {
                stmt = storage.conn.prepareStatement("INSERT INTO streamwork.dashboard(name, metrics, value, timestamp) VALUES (?, ?, ?, ?)");
                stmt.setString(1, name);
                stmt.setString(2, metrics);
                stmt.setFloat(3, value);
                stmt.setTimestamp(4, new Timestamp(ts));
                stmt.executeUpdate();
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }
}
