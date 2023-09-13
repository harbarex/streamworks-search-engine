package indexer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis455.mapreduce.worker.storage.entities.DocInfo;
import edu.upenn.cis455.mapreduce.worker.storage.entities.DocWord;
import storage.MySQLConfig;
import storage.MySQLStorage;
import edu.upenn.cis455.mapreduce.worker.storage.entities.Word;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class IndexMySQLStorage extends MySQLStorage {

    final static Logger logger = LogManager.getLogger(IndexMySQLStorage.class);

    public IndexMySQLStorage(MySQLConfig config) {

        super(config);

    }

    public void insertDocWords(ArrayList<DocWord> tasks, String tableName) {

        // tableName: either ShortDocWords or PlainDocWords

        // key: 0or1|docID|wordID

        try {

            logger.debug("Sending queries to insert into " + tableName + " ... ");

            // docID, wordID, word, tf
            Statement batchUpdates = conn.createStatement();

            for (DocWord docWord : tasks) {

                String query = "INSERT INTO " + tableName
                        + " (docID, word, tf, ntf, wordHash) VALUES "
                        + docWord.toString() + ";";

                batchUpdates.addBatch(query);

            }

            // execute
            batchUpdates.executeBatch();
            batchUpdates.close();

            logger.debug("Successfully send the query to " + tableName + " ! ");

        } catch (SQLException ex) {

            // Handle any errors
            logger.debug("Exception: " + tableName);
            logger.debug("SQLException: " + ex.getMessage());
            logger.debug("SQLState: " + ex.getSQLState());
            logger.debug("VendorError: " + ex.getErrorCode());

        }

    }

    /**
     * Insert DocWord in bulk manner
     * 
     * @param tasks
     * @param tableName
     */
    public void insertBulkDocWords(ArrayList<DocWord> tasks, String tableName) {

        // tableName: either ShortDocWords or PlainDocWords

        // key: 0or1|docID|wordID

        try {

            logger.debug("Sending queries to insert into " + tableName + " ... ");

            // docID, wordID, word, tf
            Statement batchUpdates = conn.createStatement();

            StringBuilder builder = new StringBuilder();

            builder.append("INSERT INTO " + tableName + " (docID, word, tf, ntf, wordHash) VALUES ");

            for (int i = 0; i < tasks.size(); i++) {

                DocWord docWord = tasks.get(i);

                builder.append(docWord.toString());

                if (i == (tasks.size() - 1)) {

                    builder.append(";");

                } else {

                    builder.append(",");

                }

            }

            batchUpdates.addBatch(builder.toString());

            // execute
            batchUpdates.executeBatch();
            batchUpdates.close();

            logger.debug("Successfully send the query to " + tableName + " ! ");

        } catch (SQLException ex) {

            // Handle any errors
            logger.debug("Exception: " + tableName);
            logger.debug("SQLException: " + ex.getMessage());
            logger.debug("SQLState: " + ex.getSQLState());
            logger.debug("VendorError: " + ex.getErrorCode());

        }

    }

    public void insertAndUpdateDocWords(HashMap<String, DocWord> tasks, String tableName) {

        // tableName: either ShortDocWords or PlainDocWords

        // key: 0or1|docID|wordID

        try {

            logger.debug("Sending queries to insert & update into " + tableName + " ... ");

            // docID, wordID, word, tf
            Statement batchUpdates = conn.createStatement();

            for (Map.Entry<String, DocWord> entry : tasks.entrySet()) {

                String[] keys = entry.getKey().split("|");

                String query;

                if (keys[0].equals("0")) {

                    // insert
                    query = "INSERT INTO " + tableName
                            + " (docID, word, tf, ntf, wordHash) VALUES "
                            + entry.getValue().toString() + ";";
                    // logger.debug("DocWord Q: " + query);

                } else {

                    // note - word comparison => only " and only \
                    String word = entry.getValue().getWord();
                    String printWord = word;

                    if (word.contains("\\")) {

                        printWord = printWord.replace("\\", "\\\\");

                    }

                    if (word.contains("\"")) {

                        printWord = printWord.replace("\"", "\\\"");
                    }

                    // update
                    query = "UPDATE " + tableName
                            + " SET tf = " + entry.getValue().getTf()
                            + " WHERE docID = " + entry.getValue().getDocID()
                            + " AND word = \"" + printWord + "\""
                            + " AND wordHash = " + entry.getValue().getWord().hashCode() + ";";
                    // logger.debug("DocWord Q: " + query);
                }

                batchUpdates.addBatch(query);

            }

            // execute
            batchUpdates.executeBatch();
            batchUpdates.close();

            logger.debug("Successfully send the query to " + tableName + " ! ");

        } catch (SQLException ex) {

            // Handle any errors
            logger.debug("Exception: " + tableName);
            logger.debug("SQLException: " + ex.getMessage());
            logger.debug("SQLState: " + ex.getSQLState());
            logger.debug("VendorError: " + ex.getErrorCode());

        }

    }

    public void insertLexicon(ArrayList<Word> tasks, String tableName) {

        // tableName: either ShortLexicon or PlainLexicon

        try {

            logger.debug("Sending queries to insert into " + tableName + " ... ");

            // docID, wordID, word, tf
            Statement batchUpdates = conn.createStatement();

            // return "(" + wordID + "," + "\"" + word + "\"" + "," + df + "," + idf + ")";
            for (Word wd : tasks) {

                String query = "INSERT INTO " + tableName
                        + " (wordID, word, df, idf, hitLoc) VALUES "
                        + wd.toString();

                batchUpdates.addBatch(query);

            }

            // execute
            batchUpdates.executeBatch();
            batchUpdates.close();

            logger.debug("Successfully send the INSERT query to " + tableName + " ! ");

        } catch (SQLException ex) {

            // Handle any errors
            logger.debug("Exception: " + tableName);
            logger.debug("SQLException: " + ex.getMessage());
            logger.debug("SQLState: " + ex.getSQLState());
            logger.debug("VendorError: " + ex.getErrorCode());

        }

    }

    public void insertBulkLexicon(ArrayList<Word> tasks, String tableName) {

        // tableName: either ShortLexicon or PlainLexicon

        try {

            logger.debug("Sending queries to insert into " + tableName + " ... ");

            // docID, wordID, word, tf
            Statement batchUpdates = conn.createStatement();

            // return "(" + wordID + "," + "\"" + word + "\"" + "," + df + "," + idf + ")";
            StringBuilder builder = new StringBuilder();

            builder.append("INSERT INTO " + tableName + " (wordID, word, df, idf, hitLoc) VALUES ");

            for (int i = 0; i < tasks.size(); i++) {

                Word wd = tasks.get(i);

                builder.append(wd.toString());

                if (i == (tasks.size() - 1)) {

                    builder.append(";");

                } else {

                    builder.append(",");

                }

            }

            batchUpdates.addBatch(builder.toString());

            // execute
            batchUpdates.executeBatch();
            batchUpdates.close();

            logger.debug("Successfully send the INSERT query to " + tableName + " ! ");

        } catch (SQLException ex) {

            // Handle any errors
            logger.debug("Exception: " + tableName);
            logger.debug("SQLException: " + ex.getMessage());
            logger.debug("SQLState: " + ex.getSQLState());
            logger.debug("VendorError: " + ex.getErrorCode());

        }

    }

    public void insertLexicon(HashMap<Integer, Word> tasks, String tableName) {

        // tableName: either ShortLexicon or PlainLexicon

        try {

            logger.debug("Sending queries to insert into " + tableName + " ... ");

            // docID, wordID, word, tf
            Statement batchUpdates = conn.createStatement();

            // return "(" + wordID + "," + "\"" + word + "\"" + "," + df + "," + idf + ")";
            for (Map.Entry<Integer, Word> entry : tasks.entrySet()) {

                String query = "INSERT INTO " + tableName
                        + " (wordID, word, df, idf, hitLoc) VALUES "
                        + entry.getValue().toString();

                batchUpdates.addBatch(query);

            }

            // execute
            batchUpdates.executeBatch();
            batchUpdates.close();

            logger.debug("Successfully send the INSERT query to " + tableName + " ! ");

        } catch (SQLException ex) {

            // Handle any errors
            logger.debug("Exception: " + tableName);
            logger.debug("SQLException: " + ex.getMessage());
            logger.debug("SQLState: " + ex.getSQLState());
            logger.debug("VendorError: " + ex.getErrorCode());

        }

    }

    public void updateLexicon(ArrayList<Word> tasks, String tableName) {

        try {

            logger.debug("Sending queries to update " + tableName + " ... ");

            // docID, wordID, word, tf
            Statement batchUpdates = conn.createStatement();

            for (Word wd : tasks) {

                String query = "UPDATE " + tableName
                        + " SET df = " + wd.getDf()
                        + " WHERE wordID = " + wd.getWordID();
                // logger.debug(query);
                batchUpdates.addBatch(query);

            }

            // execute
            batchUpdates.executeBatch();
            batchUpdates.close();

            logger.debug("Successfully send the UPDATE query to " + tableName + " ! ");

        } catch (SQLException ex) {

            // Handle any errors
            logger.debug("Exception: " + tableName);
            logger.debug("SQLException: " + ex.getMessage());
            logger.debug("SQLState: " + ex.getSQLState());
            logger.debug("VendorError: " + ex.getErrorCode());

        }

    }

    public void updateIDF(String docWordTableName, String lexiconTableName) {

        try {

            // docID, wordID, word, tf
            Statement batchUpdates = conn.createStatement();

            logger.debug("Sending query to update idf in " + lexiconTableName + " ... ");

            String query = "WITH TotalDoc AS ("
                    + " SELECT COUNT(DISTINCT docID) as numDocs"
                    + " FROM " + docWordTableName
                    + " ), NewLexicon AS ("
                    + " SELECT wordID as newID, LN((SELECT numDocs FROM TotalDoc) / df) AS newIDF"
                    + " FROM " + lexiconTableName
                    + " )"
                    + " UPDATE " + lexiconTableName + " AS L"
                    + " INNER JOIN NewLexicon AS newL"
                    + " ON L.wordID = newL.newID"
                    + " SET L.idf = newL.newIDF"
                    + " WHERE L.wordID = newL.newID;";

            batchUpdates.addBatch(query);

            // execute
            batchUpdates.executeBatch();
            batchUpdates.close();

            logger.debug("Successfully send the UPDATE the idf of " + lexiconTableName + " ! ");

        } catch (SQLException ex) {

            // Handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());

        }

    }

    public void updateTFIDF(String docWordTableName, String lexiconTableName) {

        try {

            // docID, wordID, word, tf
            Statement batchUpdates = conn.createStatement();

            logger.debug("Sending query to update TF-IDF ... ");

            String updateTF = "With DocL2Sum AS ("
                    + " SELECT docID, SUM(POWER(tf, 2)) AS denom"
                    + " FROM " + docWordTableName
                    + " GROUP BY docID)"
                    + ", NewNTF AS ("
                    + " SELECT S.docID, S.wordHash, S.word, CAST(SQRT(POWER(S.tf, 2) / D.denom) AS FLOAT) AS NewNTF"
                    + " FROM " + docWordTableName + " AS S LEFT JOIN DocL2Sum AS D ON S.docID = D.docID)"
                    + "UPDATE " + docWordTableName + " AS S"
                    + " INNER JOIN NewNTF AS NewS ON S.docID = NewS.docID AND S.word = NewS.word AND S.wordHash = NewS.wordHash"
                    + " SET S.ntf = NewS.newNTF"
                    + " WHERE S.docID = NewS.docID and S.word = NewS.word and S.wordHash = NewS.wordHash;";

            batchUpdates.addBatch(updateTF);

            String updateIDF = "WITH TotalDoc AS ("
                    + " SELECT COUNT(DISTINCT docID) as numDocs"
                    + " FROM " + docWordTableName
                    + " ), NewLexicon AS ("
                    + " SELECT wordID as newID, LN((SELECT numDocs FROM TotalDoc) / df) AS newIDF"
                    + " FROM " + lexiconTableName
                    + " )"
                    + " UPDATE " + lexiconTableName + " AS L"
                    + " INNER JOIN NewLexicon AS newL"
                    + " ON L.wordID = newL.newID"
                    + " SET L.idf = newL.newIDF"
                    + " WHERE L.wordID = newL.newID;";

            batchUpdates.addBatch(updateIDF);

            // execute
            batchUpdates.executeBatch();
            batchUpdates.close();

            logger.debug("Successfully send the UPDATE the TF-IDF ... ");

        } catch (SQLException ex) {

            // Handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());

        }

    }

    public void insertDocInfo(ArrayList<DocInfo> infoTasks, String tableName) {

        try {

            logger.debug("Sending queries to insert into " + tableName + " ... ");

            // docID, wordID, word, tf
            Statement batchUpdates = conn.createStatement();

            // return "(" + wordID + "," + "\"" + word + "\"" + "," + df + "," + idf + ")";
            for (DocInfo info : infoTasks) {

                String query = "INSERT INTO " + tableName
                        + " (docID, url, title, context) VALUES "
                        + info.toString();

                batchUpdates.addBatch(query);

            }

            // execute
            batchUpdates.executeBatch();
            batchUpdates.close();

            logger.debug("Successfully send the INSERT query to " + tableName + " ! ");

        } catch (SQLException ex) {

            // Handle any errors
            logger.debug("Exception: " + tableName);
            logger.debug("SQLException: " + ex.getMessage());
            logger.debug("SQLState: " + ex.getSQLState());
            logger.debug("VendorError: " + ex.getErrorCode());

        }

    }

    public void insertBulkDocInfo(ArrayList<DocInfo> infoTasks, String tableName) {

        try {

            logger.debug("Sending queries to insert into " + tableName + " ... ");

            // docID, wordID, word, tf
            Statement batchUpdates = conn.createStatement();

            // return "(" + wordID + "," + "\"" + word + "\"" + "," + df + "," + idf + ")";
            StringBuilder builder = new StringBuilder();

            builder.append("INSERT INTO " + tableName + " (docID, url, title, context) VALUES ");

            for (int i = 0; i < infoTasks.size(); i++) {

                DocInfo info = infoTasks.get(i);

                builder.append(info.toString());

                if (i == (infoTasks.size() - 1)) {

                    builder.append(";");

                } else {

                    builder.append(",");

                }

            }

            batchUpdates.addBatch(builder.toString());

            // execute
            batchUpdates.executeBatch();
            batchUpdates.close();

            logger.debug("Successfully send the INSERT query to " + tableName + " ! ");

        } catch (SQLException ex) {

            // Handle any errors
            logger.debug("Exception: " + tableName);
            logger.debug("SQLException: " + ex.getMessage());
            logger.debug("SQLState: " + ex.getSQLState());
            logger.debug("VendorError: " + ex.getErrorCode());

        }

    }

    public void insertDocInfo(HashMap<Integer, DocInfo> infoTasks, String tableName) {

        try {

            logger.debug("Sending queries to insert into " + tableName + " ... ");

            // docID, wordID, word, tf
            Statement batchUpdates = conn.createStatement();

            // return "(" + wordID + "," + "\"" + word + "\"" + "," + df + "," + idf + ")";
            for (Map.Entry<Integer, DocInfo> entry : infoTasks.entrySet()) {

                String query = "INSERT INTO " + tableName
                        + " (docID, url, title, context) VALUES "
                        + entry.getValue().toString();

                batchUpdates.addBatch(query);

            }

            // execute
            batchUpdates.executeBatch();
            batchUpdates.close();

            logger.debug("Successfully send the INSERT query to " + tableName + " ! ");

        } catch (SQLException ex) {

            // Handle any errors
            logger.debug("Exception: " + tableName);
            logger.debug("SQLException: " + ex.getMessage());
            logger.debug("SQLState: " + ex.getSQLState());
            logger.debug("VendorError: " + ex.getErrorCode());

        }

    }
}
