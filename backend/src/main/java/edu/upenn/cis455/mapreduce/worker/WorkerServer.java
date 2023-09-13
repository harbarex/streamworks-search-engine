package edu.upenn.cis455.mapreduce.worker;

import static spark.Spark.*;
import org.apache.logging.log4j.Level;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.annotation.Target;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.HttpChannelState.State;
import org.eclipse.jetty.util.ConcurrentHashSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.DistributedCluster;
import edu.upenn.cis.stormlite.LocalCluster;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.MapBolt;
import edu.upenn.cis.stormlite.bolt.PrintBolt;
import edu.upenn.cis.stormlite.bolt.ReduceBolt;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spout.ShardedFileSpout;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.mapreduce.RunJobRoute;

import edu.upenn.cis455.mapreduce.worker.storage.StorageFactory;
import edu.upenn.cis455.mapreduce.worker.storage.WorkerStorage;
import spark.Spark;

/**
 * Simple listener for worker creation
 * 
 * @author zives
 *
 */
public class WorkerServer {

    static Logger log = LogManager.getLogger(WorkerServer.class);

    static DistributedCluster cluster = new DistributedCluster();

    List<TopologyContext> contexts = new ArrayList<>();

    // keep a list of configs along with the contexts
    List<Config> configs = new ArrayList<>();

    static List<String> topologies = new ArrayList<>();

    // local storage (specifically, the DB)
    // Note1: job input / output should be under the local storage
    // Note2: make sure the job's output directory exists
    private String localStorage;
    private String localDBDirectory;
    private WorkerStorage localDBInstance;

    /**
     * Stored the info of EOS from other workers (executors).
     * Due to the broadcast nature of EOS,
     * only one broadcast from one executor is needed.
     * (In fact, each previous executor sends number of EOS
     * as the same as the number of following remote executors to the worker)
     */
    private ConcurrentHashSet<String> receivedEOS = new ConcurrentHashSet<String>();

    /**
     * Store master IP:port when initialization
     */
    private String masterAddress = null;

    public void setMaster(String address) {

        this.masterAddress = address;

    }

    /**
     * Worker port
     */
    private int port;

    /**
     * Shutdown related.
     * This is for the background thread sending /workerstatus
     */
    private boolean shouldDown = false;

    /**
     * Background thread to send /workerstatus
     */
    Runnable statusSender = new Runnable() {

        public void run() {

            // send every 10 seconds
            while (!shouldDown) {

                try {

                    // send info
                    sendWorkerStatus();

                    // sleep 10 s
                    Thread.sleep(10000);

                } catch (InterruptedException e) {

                    e.printStackTrace();
                }

            }

        }

    };

    /**
     * Thread instance for monitoring and joining.
     */
    Thread statusSenderT = new Thread(statusSender);

    /**
     * Send the info to the /workerstatus route in master.
     * TODO: encode results="xxxxx" (because of the spaces)
     */
    public void sendWorkerStatus() {

        // the query string to send
        String infoToSend = gatherWorkerInfo();

        String targetURL = this.masterAddress + "/workerstatus?" + infoToSend;

        if (!targetURL.startsWith("http://")) {

            targetURL = "http://" + targetURL;

        }

        try {

            URL url = new URL(targetURL);

            log.info("Sending status to " + url.toString());

            // open connection
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            // check resp
            int respCode = conn.getResponseCode();

            log.debug("(Worker) Send /workerstatus => response: " + respCode);

        } catch (IOException e) {

            log.debug("(Worker) Master server is not up or the master's IP:port is wrong! Send to : " + targetURL);

        }

    }

    /**
     * Gather worker info as a queryString to send.
     */
    private String gatherWorkerInfo() {

        // no job executed
        if (this.contexts.size() == 0) {

            // send dummy status
            return "port=" + this.port + "&status=IDLE&job=None&keysRead=0&keysWritten=0&results=[]";

        }

        // has some job (either doing or completed)
        StringBuilder builder = new StringBuilder();

        TopologyContext lastContext = this.contexts.get(this.contexts.size() - 1);
        Config lastConfig = this.configs.get(this.contexts.size() - 1);

        // "port=xxx&status=xxx&job=xxx&keysRead=xxx&keysWritten=xxx&results=xxxx"
        builder.append("port=" + this.port);

        builder.append("&status=" + lastContext.getState().toString());

        builder.append("&job=" + lastConfig.get("classname"));

        // // TODO: check keysRead
        // if (lastContext.getState().equals(TopologyContext.STATE.IDLE)) {

        // builder.append("&keysRead=0");

        // } else {

        // builder.append("&keysRead=" + lastContext.getMapOutputs());

        // }

        // // TOOD: check keysWritten
        // builder.append("&keysWritten=" + lastContext.getReduceOutputs());
        log.debug("Contexts: " + lastContext.getMapInputs() + " " + lastContext.getMapOutputs() + " "
                + lastContext.getReduceInputs() + " " + lastContext.getReduceOutputs());
        if (lastContext.getState().equals(TopologyContext.STATE.IDLE)) {

            builder.append("&keysRead=0");
            builder.append("&keysWritten=" + lastContext.getReduceOutputs());

        } else if (lastContext.getState().equals(TopologyContext.STATE.MAPPING)) {

            builder.append("&keysRead=" + lastContext.getMapInputs());
            builder.append("&keysWritten=" + lastContext.getMapOutputs());

        } else if (lastContext.getState().equals(TopologyContext.STATE.REDUCING)) {

            builder.append("&keysRead=" + lastContext.getReduceInputs());
            builder.append("&keysWritten=" + lastContext.getReduceOutputs());

        }

        String currentResults = getCurrentResults();

        try {

            // TODO: encode
            currentResults = URLEncoder.encode(currentResults, StandardCharsets.UTF_8.toString());

        } catch (UnsupportedEncodingException e) {

            e.printStackTrace();

        }

        builder.append("&results=" + currentResults);

        return builder.toString();
    }

    /**
     * Read localstorage/outputDir/output.txt exported by the PrinterBolt.
     * Note: up to the first 100.
     */
    private String getCurrentResults() {

        // TODO: update directory problem
        Config lastConfiig = configs.get(configs.size() - 1);

        String outputDir = lastConfiig.get("output");
        String targetOutputDirectory = configureLocalSubDirectory(localStorage, outputDir, true);

        String outputFile = targetOutputDirectory + "/" + "output.txt";

        File file = new File(outputFile);

        // check existence
        if (!file.exists()) {

            // not exist => return empty list as a string
            return "[]";

        }

        // init reader
        BufferedReader reader = null;

        try {

            reader = new BufferedReader(new FileReader(file));

        } catch (FileNotFoundException e) {

            e.printStackTrace();

        }

        if (reader == null) {

            return "[]";

        }

        StringBuilder builder = new StringBuilder();

        builder.append("[");

        try {
            int nLines = 0;
            String line = reader.readLine();

            if (line != null) {

                builder.append(line);
                nLines += 1;

            }

            while (((line = reader.readLine()) != null) && (nLines < 100)) {

                builder.append("," + line);
                nLines += 1;

            }

            reader.close();

        } catch (IOException e) {

            e.printStackTrace();

        }

        builder.append("]");

        return builder.toString();

    }

    /**
     * Clean the specified directory
     * 
     * @param directory : [String], output directory
     */
    public static void cleanDirectory(String directory) {

        if (Files.exists(Paths.get(directory))) {

            File file = new File(directory);

            StorageFactory.deleteDirectory(file);

        }

    }

    public static String configureLocalSubDirectory(String localDirectory, String subDirectory, boolean verification) {

        // subDirectory may either start with / or not
        String targetDirectory = (subDirectory.startsWith("/")) ? localDirectory + subDirectory
                : localDirectory + "/" + subDirectory;

        // shoud not end with "/"
        if (targetDirectory.endsWith("/")) {

            targetDirectory = targetDirectory.substring(0, targetDirectory.length() - 1);

        }

        // check if there is such directory or not if verification is true
        // if not exist => create one
        if (verification) {

            if (!Files.exists(Paths.get(targetDirectory))) {

                try {

                    Files.createDirectories(Paths.get(targetDirectory));

                } catch (IOException e) {

                    e.printStackTrace();

                }

            }

        }

        return targetDirectory;

    }

    // private void removePreviousTopolgy() {

    // try {

    // shutdown();
    // // cluster = new DistributedCluster();

    // // Thread.sleep(1000);

    // } catch (InterruptedException e) {

    // e.printStackTrace();

    // }

    // }

    public WorkerServer(int myPort) throws MalformedURLException {

        log.info("Creating server listener at socket " + myPort);

        port(myPort);

        this.port = myPort;

        final ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);

        Spark.post("/definejob", (req, res) -> {

            // clear the spout & bolt (in advance)
            // cluster.shutdown();
            // removePreviousTopolgy();

            WorkerJob workerJob;

            try {

                workerJob = om.readValue(req.body(), WorkerJob.class);

                // add local storage location to job's config
                // all the spouts & bolts in this server must know the worker's local directory
                workerJob.getConfig().put("storageDirectory", localStorage);
                workerJob.getConfig().put("DBDirectory", localDBDirectory);

                // check input & output subfolders
                String outputDir = workerJob.getConfig().get("output");

                // try create folder if it does not exist
                String targetOutputDirectory = configureLocalSubDirectory(localStorage, outputDir, true);

                // make sure nothing in the targetOutputDirectory
                WorkerServer.cleanDirectory(targetOutputDirectory);

                // clean received EOS
                receivedEOS.clear();

                try {

                    log.info("Processing job definition request" + workerJob.getConfig().get("job") +
                            " on machine " + workerJob.getConfig().get("workerIndex"));

                    configs.add(workerJob.getConfig());

                    contexts.add(cluster.submitTopology(workerJob.getConfig().get("job"), workerJob.getConfig(),
                            workerJob.getTopology()));

                    // Add a new topology
                    synchronized (topologies) {
                        topologies.add(workerJob.getConfig().get("job"));
                    }

                } catch (ClassNotFoundException e) {

                    // TODO Auto-generated catch block
                    e.printStackTrace();

                }

                return "Job launched";

            } catch (IOException e) {

                e.printStackTrace();

                // Internal server error
                res.status(500);
                return e.getMessage();

            }

        });

        Spark.post("/runjob", new RunJobRoute(cluster));

        Spark.post("/pushdata/:stream", (req, res) -> {

            try {

                String stream = req.params(":stream");
                log.debug("Worker received: " + req.body());
                Tuple tuple = om.readValue(req.body(), Tuple.class);

                log.debug("Worker received: " + tuple + " for " + stream);

                // Find the destination stream and route to it
                // This is a workaround to handle the problem
                // when worker receives /pushdata call when the job is still being set up
                // (May be not needed after using startSchedule() to put item in taskQueue)
                StreamRouter router = cluster.getStreamRouter(stream);
                while (router == null) {

                    // wait and then fetach again
                    try {

                        Thread.sleep(100);

                    } catch (InterruptedException e) {

                        e.printStackTrace();

                    }

                    router = cluster.getStreamRouter(stream);
                    log.debug("Initially null => get route: " + router);
                }

                if (contexts.isEmpty())
                    log.error("No topology context -- were we initialized??");

                TopologyContext ourContext = contexts.get(contexts.size() - 1);

                // Instrumentation for tracking progress
                if (!tuple.isEndOfStream())
                    ourContext.incSendOutputs(router.getKey(tuple.getValues()));

                // TODO: handle tuple vs end of stream for our *local nodes only*
                // Please look at StreamRouter and its methods (execute, executeEndOfStream,
                // executeLocally, executeEndOfStreamLocally)

                // Note
                // Each prev node would send number of next executors times
                // however, each time, it would broadcast EOS
                // So, if we've got the EOS from same source executor
                // do not process

                if (!tuple.isEndOfStream()) {

                    router.executeLocally(tuple, ourContext, tuple.getSourceExecutor());

                }

                // EOS from unseen executors
                else if (tuple.isEndOfStream() && !receivedEOS.contains(tuple.getSourceExecutor())) {

                    receivedEOS.add(tuple.getSourceExecutor());

                    // can solve the problem
                    // EOS is received earlier than the last message
                    Thread.sleep(100);

                    router.executeEndOfStreamLocally(ourContext, tuple.getSourceExecutor());

                }

                return "OK";

            } catch (IOException e) {

                // TODO Auto-generated catch block
                e.printStackTrace();

                res.status(500);
                return e.getMessage();

            }

        });

        Spark.post("/pushbatch/:stream", (req, res) -> {

            try {

                String stream = req.params(":stream");
                log.debug("Worker received batch: " + req.body());

                ArrayList<Tuple> tuples = om.readValue(req.body(), ArrayList.class);

                log.debug("Worker received batch: " + tuples.toString() + " for " + stream);

                // Find the destination stream and route to it
                // This is a workaround to handle the problem
                // when worker receives /pushdata call when the job is still being set up
                // (May be not needed after using startSchedule() to put item in taskQueue)
                StreamRouter router = cluster.getStreamRouter(stream);

                while (router == null) {

                    // wait and then fetach again
                    try {

                        Thread.sleep(100);

                    } catch (InterruptedException e) {

                        e.printStackTrace();

                    }

                    router = cluster.getStreamRouter(stream);
                    log.debug("Initially null => get route: " + router);
                }

                if (contexts.isEmpty())
                    log.error("No topology context -- were we initialized??");

                TopologyContext ourContext = contexts.get(contexts.size() - 1);

                for (Tuple tuple : tuples) {

                    // Instrumentation for tracking progress
                    if (!tuple.isEndOfStream())
                        ourContext.incSendOutputs(router.getKey(tuple.getValues()));

                    // TODO: handle tuple vs end of stream for our *local nodes only*
                    // Please look at StreamRouter and its methods (execute, executeEndOfStream,
                    // executeLocally, executeEndOfStreamLocally)

                    // Note
                    // Each prev node would send number of next executors times
                    // however, each time, it would broadcast EOS
                    // So, if we've got the EOS from same source executor
                    // do not process

                    if (!tuple.isEndOfStream()) {

                        router.executeLocally(tuple, ourContext, tuple.getSourceExecutor());

                    }

                    // EOS from unseen executors
                    else if (tuple.isEndOfStream() && !receivedEOS.contains(tuple.getSourceExecutor())) {

                        receivedEOS.add(tuple.getSourceExecutor());

                        // can solve the problem
                        // EOS is received earlier than the last message
                        Thread.sleep(500);

                        router.executeEndOfStreamLocally(ourContext, tuple.getSourceExecutor());

                    }
                }

                return "OK";

            } catch (IOException e) {

                // TODO Auto-generated catch block
                e.printStackTrace();

                res.status(500);
                return e.getMessage();

            }

        });

        Spark.get("/shutdown", (req, res) -> {

            log.debug("Received shutdown!!");

            // killtopology
            shutdown();

            this.shouldDown = true;

            // stop the workerstatus thread
            try {

                this.statusSenderT.join(1000);

            } catch (InterruptedException e) {

                e.printStackTrace();
            }

            // close DB
            localDBInstance.close();

            // remove db directory
            StorageFactory.removeWorkerStorage(this.localDBDirectory);

            // stop server
            stop();

            return "(Worker) Successfully shutdown!";

        });

        // run statusSender
        statusSenderT.start();

    }

    /**
     * Set up the directory for local storage.
     * Create database in this local directory.
     * 
     * @param directory : [String]
     */
    public void setupLocalStorage(String directory) {

        this.localStorage = directory;

        this.localDBDirectory = directory + "/" + "DB";

        // create database
        this.localDBInstance = StorageFactory.getDatabase(this.localDBDirectory);

    }

    public static void shutdown() {

        synchronized (topologies) {
            for (String topo : topologies)
                cluster.killTopology(topo);
        }

        cluster.shutdown();
    }

    public static void createWorker(Map<String, String> config) {

        if (!config.containsKey("workerList"))
            throw new RuntimeException("Worker spout doesn't have list of worker IP addresses/ports");

        if (!config.containsKey("workerIndex"))
            throw new RuntimeException("Worker spout doesn't know its worker ID");

        else {

            String[] addresses = WorkerHelper.getWorkers(config);
            String myAddress = addresses[Integer.valueOf(config.get("workerIndex"))];

            log.debug("Initializing worker " + myAddress);

            URL url;

            // TODO: check local storage location
            if (!config.containsKey("storageDirectory"))
                throw new RuntimeException("Worker doesn't have a local directory for storage");

            String localDirectory = config.get("storageDirectory");

            try {

                url = new URL(myAddress);

                WorkerServer server = new WorkerServer(url.getPort());
                server.setupLocalStorage(localDirectory);

            } catch (MalformedURLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
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

    public void testLocalSendAndRun() {

        String wordSpout = "WORD_SPOUT";
        String wordMapper = "WORD_MAP";
        String wordReducer = "WORD_REDUCE";
        String wordPrinter = "WORD_PRINTER";

        ShardedFileSpout spout = new ShardedFileSpout();
        MapBolt mapBolt = new MapBolt();
        ReduceBolt reduceBolt = new ReduceBolt();
        PrintBolt printerBolt = new PrintBolt();

        Config config = new Config();

        config.put("workerList", "[127.0.0.1:8001]");

        // Job name
        config.put("job", "test");

        // Class with map function
        config.put("mapClass", "edu.upenn.cis455.mapreduce.job.WordCount");
        // Class with reduce function
        config.put("reduceClass", "edu.upenn.cis455.mapreduce.job.WordCount");

        // Numbers of executors (per node)
        config.put("spoutExecutors", "1");
        config.put("mapExecutors", "5");
        config.put("reduceExecutors", "3");

        config.put("workerIndex", "0");

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(wordSpout, spout, Integer.parseInt(config.get("spoutExecutors")));
        builder.setBolt(wordMapper, mapBolt, Integer.parseInt(config.get("mapExecutors"))).fieldsGrouping(wordSpout,
                new Fields("key"));
        builder.setBolt(wordReducer, reduceBolt, Integer.parseInt(config.get("reduceExecutors")))
                .fieldsGrouping(wordMapper, new Fields("key"));
        builder.setBolt(wordPrinter, printerBolt, 1).firstGrouping(wordReducer);

        Topology topo = builder.createTopology();

        WorkerJob job = new WorkerJob(topo, config);

        ObjectMapper mapper = new ObjectMapper();
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        try {
            String[] workers = WorkerHelper.getWorkers(config);

            int i = 0;
            for (String dest : workers) {
                config.put("workerIndex", String.valueOf(i++));
                if (sendJob(dest, "POST", config, "definejob",
                        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job))
                        .getResponseCode() != HttpURLConnection.HTTP_OK) {
                    throw new RuntimeException("Job definition request failed");
                }
            }
            for (String dest : workers) {
                if (sendJob(dest, "POST", config, "runjob", "").getResponseCode() != HttpURLConnection.HTTP_OK) {
                    throw new RuntimeException("Job execution request failed");
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(0);
        }

    }

    private static void localTestSetupAndRun(String directory) {

        String wordSpout = "WORD_SPOUT";
        String wordMapper = "WORD_MAP";
        String wordReducer = "WORD_REDUCE";
        String wordPrinter = "WORD_PRINTER";

        ShardedFileSpout spout = new ShardedFileSpout();
        MapBolt mapBolt = new MapBolt();
        ReduceBolt reduceBolt = new ReduceBolt();
        PrintBolt printerBolt = new PrintBolt();

        Config config = new Config();

        config.put("workerList", "[127.0.0.1:8001]");

        // Job name
        config.put("job", "test");

        // Class with map function
        config.put("mapClass", "edu.upenn.cis455.mapreduce.job.WordCount");
        // Class with reduce function
        config.put("reduceClass", "edu.upenn.cis455.mapreduce.job.WordCount");

        // Numbers of executors (per node)
        config.put("spoutExecutors", "1");
        config.put("mapExecutors", "1");
        config.put("reduceExecutors", "5");

        config.put("workerIndex", "1");
        config.put("storageDirectory", directory);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(wordSpout, spout, Integer.parseInt(config.get("spoutExecutors")));
        builder.setBolt(wordMapper, mapBolt, Integer.parseInt(config.get("mapExecutors"))).fieldsGrouping(wordSpout,
                new Fields("key"));
        builder.setBolt(wordReducer, reduceBolt, Integer.parseInt(config.get("reduceExecutors")))
                .fieldsGrouping(wordMapper, new Fields("key"));
        builder.setBolt(wordPrinter, printerBolt, 1).firstGrouping(wordReducer);

        Topology topo = builder.createTopology();

        WorkerJob job = new WorkerJob(topo, config);

        // LocalCluster cluster = new LocalCluster();

        try {

            // cluster.submitTopology(job.getConfig().get("job"), job.getConfig(),
            // job.getTopology());
            cluster.submitTopology(job.getConfig().get("job"), job.getConfig(), builder.createTopology());
            cluster.startTopology();
            Thread.sleep(30000);
            cluster.killTopology(job.getConfig().get("job"));
            cluster.shutdown();

        } catch (ClassNotFoundException | InterruptedException e) {

            e.printStackTrace();

        }

    }

    /**
     * Simple launch for worker server. Note that you may want to change / replace
     * most of this.
     * 
     * @param args
     * @throws MalformedURLException
     */
    public static void main(String args[]) throws MalformedURLException {

        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn", Level.DEBUG);

        if (args.length < 3) {
            System.out.println("Usage: WorkerServer [port number] [master host/IP]:[master port] [storage directory]");
            System.exit(1);
        }

        int myPort = Integer.valueOf(args[0]);

        System.out.println("Worker node startup, on port " + myPort);

        WorkerServer worker = new WorkerServer(myPort);

        // set master IP:port
        worker.setMaster(args[1]);

        // set up local storage
        worker.setupLocalStorage(args[2]);

        // TODO: you may want to adapt parts of
        // edu.upenn.cis.stormlite.mapreduce.TestMapReduce here

        // Should be => Launch server => master send post (define job)
        // worker.testLocalSendAndRun();
        // localTestSetupAndRun(args[2]);

    }
}
