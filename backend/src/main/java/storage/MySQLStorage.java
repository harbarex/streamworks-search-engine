package storage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MySQLStorage {
    final static Logger logger = LogManager.getLogger(MySQLStorage.class);

    public Connection conn = null;

    public MySQLStorage(MySQLConfig config) {
        setConnection(config);
    }

    /**
     * Dummy query to keep connected
     */
    public void sendDummyQuery() {
        try {
            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery("SELECT 1");
            resultSet.close();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void setConnection(MySQLConfig config) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");

            String jdbcUrl = "jdbc:mysql://"
                    + config.getHostname() + ":" + config.getPort()
                    + "/" + config.getDbname()
                    + "?user=" + config.getUsername()
                    + "&password=" + config.getPassword();

            logger.debug("Getting connection : " + jdbcUrl + " ... ");

            Properties connProperties = new Properties();
            connProperties.put("password", config.getPassword());
            connProperties.put("user", config.getUsername());
            connProperties.put("autoReconnect", "true");
            connProperties.put("maxReconnects", "4");

            conn = DriverManager.getConnection(jdbcUrl, connProperties);
            logger.debug("Remote connection successful.");
        } catch (ClassNotFoundException e) {
            logger.warn(e.toString());
        } catch (SQLException e) {
            logger.warn(e.toString());
        }
    }

    public void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }
}
