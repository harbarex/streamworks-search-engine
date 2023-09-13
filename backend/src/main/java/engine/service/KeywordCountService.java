package engine.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import engine.entity.PopularKeyword;
import storage.MySQLStorage;

public class KeywordCountService {
    private static MySQLStorage storage = null;

    public static void init(MySQLStorage db) {
        storage = db;
    }

    public static void incCount(String keyword) throws SQLException {
        Connection conn = storage.conn;
        conn.setAutoCommit(false);
        PreparedStatement stmt = conn.prepareStatement("SELECT * FROM streamwork.popularkeyword WHERE keyword = ?");
        stmt.setString(1, keyword);
        ResultSet resultSet = stmt.executeQuery();
        PopularKeyword popularKeyword = null;
        if (resultSet.next()) {
            popularKeyword = new PopularKeyword(
                resultSet.getString("keyword"),
                resultSet.getInt("count")
            );
        }
        PreparedStatement updateStmt = null;
        if (popularKeyword != null) {
            updateStmt = conn.prepareStatement("UPDATE streamwork.popularkeyword SET count = ? WHERE keyword = ?");
            updateStmt.setInt(1, popularKeyword.getCount() + 1);
            updateStmt.setString(2, popularKeyword.getKeyword());
        } else {
            updateStmt = conn.prepareStatement("INSERT INTO streamwork.popularkeyword VALUES (?, ?)");
            updateStmt.setString(1, keyword);
            updateStmt.setInt(2, 1);
        }
        updateStmt.executeUpdate();
        conn.commit();
        updateStmt.close();
        resultSet.close();
        stmt.close();
    }

    public static List<PopularKeyword> getPopularKeywords(int top) throws SQLException {
        List<PopularKeyword> keywords = new ArrayList<>();
        Statement stmt = storage.conn.createStatement();
        ResultSet resultSet = stmt.executeQuery("SELECT * FROM streamwork.popularkeyword ORDER BY count DESC LIMIT " + top);

        while (resultSet.next()) {
            PopularKeyword popularKeyword = new PopularKeyword(
                resultSet.getString("keyword"),
                resultSet.getInt("count")
            );
            keywords.add(popularKeyword);
        }
        return keywords;
    }
}
