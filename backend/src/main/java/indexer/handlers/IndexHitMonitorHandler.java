package indexer.handlers;

import spark.HaltException;
import static spark.Spark.*;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;

import spark.Request;
import spark.Response;
import spark.Route;

import indexer.scorer.IndexScorer;

public class IndexHitMonitorHandler implements Route {

    private IndexScorer indexScorer;

    public IndexHitMonitorHandler(IndexScorer indexScorer) {

        this.indexScorer = indexScorer;

    }

    public boolean isAccessible(String ipAddress, String hitGroup) {

        // API: /access/:hitname
        try {

            URL url = new URL("http://" + ipAddress + "/access/" + hitGroup);

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {

                // check response

                return true;

            }

        } catch (java.net.ConnectException | java.net.MalformedURLException e) {

            // e.printStackTrace();
            System.out.println("Address: " + ipAddress + " not acccessible for " + hitGroup + " ! ");

        } catch (IOException e) {

            System.out.println("Address: " + ipAddress + " not acccessible for " + hitGroup + " ! ");

        }

        return false;

    }

    private String renderMonitor() {

        StringBuilder builder = new StringBuilder();

        builder.append("<html>");
        builder.append("<body style=\"background-color:#EAE7DC\">");
        builder.append("<div style=\"text-align:center\">");
        builder.append("<h1>Indexer Nodes Monitor</h1>");
        builder.append("<hr>");

        // table
        builder.append(
                "<table style=\"border:1px solid black;margin-left:auto;margin-right:auto;\" cellspacing=\"3\" bgcolor=\"#000000\"><tr bgcolor=\"#ffffff\"><th>Node ID</th><th>Node State</th></tr>");

        // content
        HashMap<String, ArrayList<String>> remoteHitAddresses = this.indexScorer.getRemoteHitAdddresses();

        int nShards = remoteHitAddresses.size();

        for (int i = 0; i < nShards; i++) {

            builder.append("<tr bgcolor=\"#ffffff\">");
            // the showing thread number starts from 1

            builder.append("<td style=\"text-align:center \">" + "hit" + i + "</td>");

            ArrayList<String> targetAddresses = remoteHitAddresses.get("hit" + i);

            builder.append("<td style=\"text-align:center \">");

            builder.append("[");

            for (int j = 0; j < targetAddresses.size(); j++) {

                boolean accessible = isAccessible(targetAddresses.get(j), "hit" + i);

                if (j > 0) {

                    builder.append(" , ");

                }

                if (accessible) {

                    // green
                    builder.append("<span style=\"color:green\">" + targetAddresses.get(j) + "</span>");

                } else {

                    // red
                    builder.append("<span style=\"color:red\">" + targetAddresses.get(j) + "</span>");

                }

            }

            builder.append("]");

            builder.append("</td>");

            builder.append("</tr>");

        }

        // table end
        builder.append("</table>");

        builder.append("</div>");
        builder.append("</body>");
        builder.append("</html>");

        return builder.toString();

    }

    @Override
    public Object handle(Request req, Response resp) throws HaltException {

        return renderMonitor();
    }

}
