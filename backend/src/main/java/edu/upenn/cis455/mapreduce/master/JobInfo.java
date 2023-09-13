package edu.upenn.cis455.mapreduce.master;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.PatternSyntaxException;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;

public class JobInfo {

    private Config config;

    private HashMap<String, String> info = new HashMap<String, String>() {
        {

            put("keysWritten", "0");
            put("results", "[]"); // printed as a list e.g. [(a, 1),(b, 1)]

        }

    };

    private int numWorkers;
    private HashSet<String> eosWorkers = new HashSet<String>();
    private boolean isDone = false;

    /**
     * This class parses the queryString and retrieve required info.
     */
    public JobInfo(Config config) {

        this.config = config;

        String[] existingWorkers = WorkerHelper.getWorkers(config);

        numWorkers = existingWorkers.length;

    }

    public boolean jobIsDone() {

        return isDone;

    }

    public Config getConfig() {
        return config;
    }

    public void updateEOJ(String worker) {

        synchronized (eosWorkers) {

            eosWorkers.add(worker);

            if (eosWorkers.size() >= this.numWorkers) {

                isDone = true;

            }

        }

    }

    public void updateJobInfo(String query) {

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

    public String toString() {

        return "ID: " + this.config.get("jobID")
                + ",\tName: " + this.config.get("job")
                + ",\tJob: " + this.config.get("classname");

    }

}
