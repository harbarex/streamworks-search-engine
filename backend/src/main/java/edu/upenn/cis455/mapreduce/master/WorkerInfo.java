package edu.upenn.cis455.mapreduce.master;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.PatternSyntaxException;

public class WorkerInfo {

    private HashMap<String, String> info = new HashMap<String, String>() {
        {

            put("port", null);
            put("status", "IDLE");
            put("job", "None");
            put("keysRead", "0");
            put("keysWritten", "0");
            put("results", "[]"); // printed as a list e.g. [(a, 1),(b, 1)]

        }
    };

    // in ms
    private long lastReceived;

    /**
     * This class parses the queryString and retrieve required info.
     */
    public WorkerInfo(String query) {

        // set up info
        parseQuery(query);

        // set last Received (ms)
        this.lastReceived = getCurrentDate().getTime();

    }

    public void parseQuery(String query) {

        try {

            // step1: split by "&"
            String[] params = query.split("&");

            for (String p : params) {

                int loc = p.indexOf("=");

                try {

                    String key = p.substring(0, loc).strip();

                    String value = p.substring(loc + 1).strip();

                    // deal with results (decode the value)
                    if (key.equals("results")) {

                        value = URLDecoder.decode(value, StandardCharsets.UTF_8.toString());

                    }

                    if (info.containsKey(key)) {

                        info.put(key, value);

                    }

                } catch (NullPointerException | IndexOutOfBoundsException | UnsupportedEncodingException e) {

                    e.printStackTrace();

                    // if format error => no update for this key

                }

            }

        } catch (PatternSyntaxException e) {

            e.printStackTrace();

        }

    }

    /**
     * Update all the fields.
     * 
     * @param query
     */
    public void update(String query) {

        parseQuery(query);

        // update last Received
        this.lastReceived = getCurrentDate().getTime();

    }

    /**
     * Get current time as Date object
     * 
     * @return [Date]
     */
    public static Date getCurrentDate() {

        Calendar c = Calendar.getInstance();

        return c.getTime();

    }

    /**
     * Check active or not
     */
    public boolean isActive() {

        // Active: current time - lastReceived <= 30 s
        long bound = 30000;

        long current = getCurrentDate().getTime();

        if ((current - this.lastReceived) > bound) {

            // inactive
            return false;

        }

        return true;

    }

    public String toString() {

        return "port=" + this.info.get("port")
                + ",\tstatus=" + this.info.get("status")
                + ",\tjob=" + this.info.get("job")
                + ",\tkeysRead=" + this.info.get("keysRead")
                + ",\tkeysWritten=" + this.info.get("keysWritten")
                + ",\tresults=" + this.info.get("results");

    }

}
