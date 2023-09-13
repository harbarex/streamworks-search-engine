package edu.upenn.cis.stormlite.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.spout.FileSpout;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;

public class ShardedFileSpout extends FileSpout {

    private String targetInputDirectory;
    private String workerIndex;

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {

        this.collector = collector;

        // target subfolder
        String localStorage = (String) conf.get("storageDirectory");
        String inputDir = (String) conf.get("input");
        targetInputDirectory = WorkerServer.configureLocalSubDirectory(localStorage, inputDir, false);

        // worker index
        workerIndex = (String) conf.get("workerIndex");

        log.debug("(Spout) Index: " + workerIndex + ", Target subdirectory: " + targetInputDirectory);

        try {

            String filename = getFilename();

            if (filename == null) {

                throw new FileNotFoundException();

            }

            log.debug("Starting spout for " + filename + " on worker (Index " + workerIndex + ") " + getExecutorId());

            reader = new BufferedReader(new FileReader(filename));

        } catch (FileNotFoundException e) {

            // TODO Auto-generated catch block
            e.printStackTrace();

        }

    }

    /**
     * Get the original filename.
     * During open, each worker adds its Index
     * following the filename to read its sharded file.
     */
    @Override
    public String getFilename() {

        // look for its sharded file in the subdirectory
        String filename = null;

        File[] files = new File(targetInputDirectory).listFiles();

        // iterate through the files in the list (because the filename is unknown)
        for (File f : files) {

            // log.debug("(Spout) Traverse: " + f.toString());
            if (f.toString().endsWith("." + workerIndex)) {

                filename = f.toString();

                break;

            }

        }

        return filename;

    }

}
