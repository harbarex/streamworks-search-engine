package edu.upenn.cis455.mapreduce.master;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static spark.Spark.*;

import spark.HaltException;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.tuple.Fields;

import edu.upenn.cis.stormlite.bolt.IndexUpdateBolt;
import edu.upenn.cis.stormlite.bolt.IndexMapBolt;
import edu.upenn.cis.stormlite.bolt.IndexReduceBolt;

import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.spout.ShardedFileSpout;
import edu.upenn.cis.stormlite.spout.ShortFileSpout;
import edu.upenn.cis.stormlite.spout.IndexFileSpout;

public class IndexerMasterServer {

    static Logger log = LogManager.getLogger(IndexerMasterServer.class);

    /**
     * Stored worker info (retrieved from /workerstatus)
     */
    public static LinkedHashMap<String, WorkerInfo> workers = new LinkedHashMap<String, WorkerInfo>();

    public static String indexStorageDirectory;

    /**
     * Transform a set of workers into a predefined string.
     * Note: Get the active workers.
     * 
     * @return [String]
     */
    public static String getWorkerList() {

        StringBuilder builder = new StringBuilder();

        // null if it is empty worker set
        synchronized (workers) {

            if (workers.size() == 0) {

                return null;

            }

            // start build the workerlist
            builder.append("[");

            List<String> workerlist = new ArrayList<String>(workers.keySet());

            int activeWorkers = 0;

            for (String worker : workerlist) {

                // check active or not
                WorkerInfo info = workers.get(worker);

                if (!info.isActive()) {

                    // not active
                    continue;

                }

                activeWorkers += 1;

                builder.append(worker);

                builder.append(",");

            }

            // remove last "," & add "]"
            if (activeWorkers > 0) {

                builder.setLength(builder.length() - 1);

            }

            builder.append("]");

        }

        return builder.toString();
    }

    /**
     * Register the /status page.
     */
    public static void registerStatusPage() {

        get("/status", (request, response) -> {

            response.type("text/html");

            // get active worker
            String workerStatus = getActiveWorkersInfo();

            String form = "<h3>Submit a Job</h3><br/>"
                    + "<form method=\"POST\" action=\"/submitjob\"> "
                    + "Job Name: <input type=\"text\" name=\"jobname\"/><br/> "
                    + "Class Name: <input type=\"text\" name=\"classname\"/><br/> "
                    + "Spout Type: <input type=\"text\" name=\"spout\"/><br/> "
                    + "Input Directory: <input type=\"text\" name=\"input\"/><br/> "
                    + "Output Directory: <input type=\"text\" name=\"output\"/><br/> "
                    + "Map Threads: <input type=\"text\" name=\"map\"/><br/> "
                    + "Reduce Threads: <input type=\"text\" name=\"reduce\"/><br/> "
                    + "<input type=\"submit\" value=\"Submit\"/>"
                    + "</form>";

            return ("<html><head><title>Master</title></head>\n" +
                    "<body>Hi, I am Chun-Fu Yeh (cfyeh)!"
                    + "<br/>"
                    + "<br/>"
                    + "<h3>Active Workers</h3>"
                    + "<div>" + workerStatus
                    + "</div>"
                    + "<br/>"
                    + form
                    + "</body></html>");

        });

    }

    /**
     * Register POST route : /submitjob
     */
    public static void registerSubmitJob() {

        post("/submitjob", (req, res) -> {

            ArrayList<String> targetParams = new ArrayList<String>() {
                {
                    add("jobname");
                    add("classname");
                    add("input");
                    add("output");
                    add("spout");
                    add("map");
                    add("reduce");
                }
            };

            HashMap<String, String> paramsMap = new HashMap<String, String>();

            // check exist or not
            for (String param : targetParams) {

                String value = req.queryParamOrDefault(param, null);

                if (value == null) {

                    // error message
                    halt(400, "No value found for the key: " + param + " ! Please check!");

                }

                paramsMap.put(param, value);

            }

            Config config = generateConfig(paramsMap);

            // get instance of a worker job
            WorkerJob job = buildWorkerJob(config);

            // send to worker's /definejob
            postJobToWorkers(job, config);

            return "Successfully submit a job!";

        });

    }

    /**
     * Helper method to generate config based on the user's query params.
     * 
     * @param paramsMap : [<String, String>].
     *                  must include
     *                  'jobname', 'classname',
     *                  'input', 'output',
     *                  'map', 'reduce',
     * @return
     */
    public static Config generateConfig(HashMap<String, String> paramsMap) {

        // create job config
        Config config = new Config();

        // check worker list
        String workerList = getWorkerList();

        if (workerList.equals("[]")) {

            // 400
            halt(400, "No workers available now! Please wait for workers to set up!");

        }

        config.put("workerList", workerList);

        config.put("job", paramsMap.get("jobname"));

        // add class name for convenience
        config.put("classname", paramsMap.get("classname"));
        config.put("spoutType", paramsMap.get("spout"));

        config.put("mapClass", paramsMap.get("classname"));
        config.put("reduceClass", paramsMap.get("classname"));
        config.put("mapExecutors", paramsMap.get("map"));
        config.put("reduceExecutors", paramsMap.get("reduce"));

        config.put("input", paramsMap.get("input"));
        config.put("output", paramsMap.get("output"));

        config.put("spoutExecutors", "1"); // default to 1
        config.put("printerExecutors", "1"); // default to 1

        // global config (for word index), visited docs
        config.put("indexStorage", indexStorageDirectory);

        return config;

    }

    /**
     * Build the topology and subsequently create worker job.
     * 
     * @param config : [<String, String>]
     * @return [WorkerJob]
     */
    public static WorkerJob buildWorkerJob(Config config) {

        String wordSpout = "WORD_SPOUT";
        String wordMapper = "WORD_MAP";
        String wordReducer = "WORD_REDUCE";
        String wordPrinter = "WORD_PRINTER";

        // ShortFileSpout spout = new ShortFileSpout();
        IndexFileSpout spout = new IndexFileSpout();

        IndexMapBolt mapBolt = new IndexMapBolt();

        IndexReduceBolt reduceBolt = new IndexReduceBolt();

        IndexUpdateBolt printerBolt = new IndexUpdateBolt();

        TopologyBuilder builder = new TopologyBuilder();

        try {

            builder.setSpout(wordSpout, spout,
                    Integer.parseInt(config.get("spoutExecutors")));

            builder.setBolt(wordMapper, mapBolt,
                    Integer.parseInt(config.get("mapExecutors"))).fieldsGrouping(wordSpout, new Fields("key"));

            builder.setBolt(wordReducer, reduceBolt,
                    Integer.parseInt(config.get("reduceExecutors"))).fieldsGrouping(wordMapper, new Fields("key"));

            builder.setBolt(wordPrinter, printerBolt,
                    Integer.parseInt(config.get("printerExecutors"))).firstGrouping(wordReducer);

        }

        catch (NumberFormatException e) {

            e.printStackTrace();
            halt(400, "Number Format Problem : number of executors for Map & reduce should be an integer!");

        }

        Topology topo = builder.createTopology();

        WorkerJob job = new WorkerJob(topo, config);

        return job;

    }

    /**
     * Post the job to worker's /definejob
     * 
     * @param job    : [WorkerJob]
     * @param config : [Config]
     */
    public static void postJobToWorkers(WorkerJob job, Config config) {

        ObjectMapper mapper = new ObjectMapper();

        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);

        try {

            String[] existingWorkers = WorkerHelper.getWorkers(config);

            int i = 0;

            for (String dest : existingWorkers) {

                config.put("workerIndex", String.valueOf(i++));

                if (sendJob(dest, "POST", config, "definejob",
                        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job))
                        .getResponseCode() != HttpURLConnection.HTTP_OK) {

                    throw new RuntimeException("Job definition request failed");

                }
            }

            try {

                // try make some time for workers to set up
                log.debug("(Master) wait for 500 ms to have the workers set up!");
                Thread.sleep(300);

            } catch (InterruptedException e) {

                e.printStackTrace();

            }

            for (String dest : existingWorkers) {

                if (sendJob(dest, "POST", config, "runjob", "").getResponseCode() != HttpURLConnection.HTTP_OK) {

                    throw new RuntimeException("Job execution request failed");

                }

            }

        } catch (IOException e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
            // System.exit(0);
            halt(404, "Unable to connect to the workers! Please try later or check the status of the workers!");

        }

    }

    static HttpURLConnection sendJob(String dest, String reqType, Config config, String job, String parameters)
            throws IOException {
        URL url = new URL(dest + "/" + job);

        log.info("Sending request to " + url.toString());

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod(reqType);

        if (reqType.equals("POST")) {
            conn.setRequestProperty("Content-Type", "application/json");

            OutputStream os = conn.getOutputStream();
            byte[] toSend = parameters.getBytes();
            os.write(toSend);
            os.flush();
        } else
            conn.getOutputStream();

        return conn;
    }

    /**
     * Route for worker to send to periodically. (Route: /workerstatus)
     */
    public static void registerWorkerStatus() {

        get("/workerstatus", (req, res) -> {

            // client IP
            String ip = req.ip();

            // requirements: port, status, job, keysRead, keysWritten, results
            // get client port from queryParam
            String portS = req.queryParamOrDefault("port", null);

            if (portS == null) {

                // no port => bad request
                halt(400, "No port specified in the queryString!");

            }

            int port = Integer.parseInt(portS);

            String query = req.queryString();

            log.debug("Got query: " + query);

            String key = ip + ":" + port;

            // deal with inactive to active mode
            if (!workers.containsKey(key)) {

                // init
                workers.put(key, new WorkerInfo(query));

            }

            else if (!workers.get(key).isActive()) {

                // from inactive to active => move to last
                workers.remove(key);

                // add new one
                workers.put(key, new WorkerInfo(query));

            }

            // already exist (and in active mode)
            else {

                WorkerInfo existedInfo = workers.get(key);
                existedInfo.update(query);

            }

            return "Successfully update a worker status!";

        });

    }

    /**
     * Gather the info of all active workers to show in status page.
     * 
     * @return [String] , html content
     */
    public static String getActiveWorkersInfo() {

        StringBuilder builder = new StringBuilder();

        synchronized (workers) {

            List<String> workerlist = new ArrayList<String>(workers.keySet());

            int activeWorkers = 0;

            for (String worker : workerlist) {

                // check active or not
                WorkerInfo info = workers.get(worker);

                if (!info.isActive()) {

                    // not active
                    continue;

                }

                // add to status
                builder.append("" + activeWorkers + ":\t" + info.toString() + "<br/>");

                activeWorkers += 1;

            }

        }

        return builder.toString();

    }

    // TODO: Shutdown routes & worker's
    public static void registerShutdown() {

        get("/shutdown", (req, res) -> {

            // shutdown workers
            sendShutdownToWorkers();

            // shutdown server
            try {

                Thread.sleep(1000);

            } catch (InterruptedException e) {

                e.printStackTrace();

            }

            stop();

            return "Successfully shutdown!";
        });

    }

    public static void sendShutdownToWorkers() {

        synchronized (workers) {

            for (String workerAddress : workers.keySet()) {

                try {

                    URL url = new URL("http://" + workerAddress + "/shutdown");

                    log.debug("Send shutdown to : " + url.toString());

                    HttpURLConnection conn = (HttpURLConnection) url.openConnection();

                    int resp = conn.getResponseCode();

                } catch (IOException e) {

                    e.printStackTrace();
                }

            }
        }

    }

    /**
     * The mainline for launching a MapReduce Master. This should
     * handle at least the status and workerstatus routes, and optionally
     * initialize a worker as well.
     * 
     * @param args
     */
    public static void main(String[] args) {

        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn", Level.DEBUG);

        if (args.length < 2) {
            System.out.println("Usage: MasterServer [port number] [index storage local directory]");
            System.exit(1);
        }

        int myPort = Integer.parseInt(args[0]);
        port(myPort);
        System.out.println("Master node startup, on port " + myPort);

        indexStorageDirectory = args[1];
        System.out.println("Master's main index storage: " + indexStorageDirectory);

        // TODO: you may want to adapt parts of
        // edu.upenn.cis.stormlite.mapreduce.TestMapReduce here
        registerStatusPage();

        // TODO: route handler for /workerstatus reports from the workers
        registerSubmitJob();
        registerWorkerStatus();
        registerShutdown();

    }
}
