package edu.upenn.cis.stormlite.bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.distributed.ConsensusTracker;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;
import edu.upenn.cis455.mapreduce.worker.storage.WorkerStorage;

/**
 * A trivial bolt that simply outputs its input stream to the
 * console
 * 
 * @author zives
 *
 */
public class PrintBolt implements IRichBolt {

	static Logger log = LogManager.getLogger(PrintBolt.class);

	Fields myFields = new Fields();

	/**
	 * To make it easier to debug: we have a unique ID for each
	 * instance of the PrintBolt, aka each "executor"
	 */
	String executorId = UUID.randomUUID().toString();

	/**
	 * target output directory (under worker's local storage)
	 */
	private String targetOutputDirectory;
	private String outputFilename = "output.txt";

	/**
	 * This is where we send our output stream
	 */
	private OutputCollector collector;
	private TopologyContext context;

	/**
	 * This object can help determine when we have
	 * reached enough votes for EOS
	 */
	ConsensusTracker votesForEos;

	HashMap<String, String> localRecords = new HashMap<String, String>();

	@Override
	public void cleanup() {

		// Do nothing
		localRecords.clear();

	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {

		this.collector = collector;
		this.context = context;

		String localDirectory = stormConf.get("storageDirectory");
		String outputDir = stormConf.get("output");

		targetOutputDirectory = WorkerServer.configureLocalSubDirectory(localDirectory, outputDir, true);

		// // make sure nothing in the targetOutputDirectory
		// WorkerServer.cleanDirectory(targetOutputDirectory);

		// number of workers
		String[] workers = WorkerHelper.getWorkers(stormConf);
		int numWorkers = workers.length;

		// How many map in the topo
		int numReduces = Integer.parseInt(stormConf.get("reduceExecutors"));

		log.debug("(PrinterBolt) Index: " + stormConf.get("workerIndex")
				+ " , wait for " + numWorkers + " * " + numReduces + " (n) EOS");

		votesForEos = new ConsensusTracker(numReduces * numWorkers);

	}

	@Override
	public boolean execute(Tuple input) {

		if (!input.isEndOfStream()) {

			System.out.println(getExecutorId() + ": " + input.toString());

			// add temporary storage
			localRecords.put(input.getStringByField("key"), input.getStringByField("value"));

		} else {

			// add EOS & check
			if (votesForEos.voteForEos(input.getSourceExecutor())) {

				// reach criteria
				// write output.txt in local storage
				this.writeToFile();

				// set state to IDLE
				this.context.setState(TopologyContext.STATE.IDLE);

				log.debug("(PrinterBolt) " + executorId + " : export "
						+ targetOutputDirectory + "/" + outputFilename + "! Become IDLE!");

			}

		}

		return true;
	}

	public void writeToFile() {

		try {

			String filename = targetOutputDirectory + "/" + outputFilename;

			log.debug("(PrinterBolt) Target Filename (with subdirectory) : " + filename);

			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));

			for (Map.Entry<String, String> entry : this.localRecords.entrySet()) {

				writer.write("(" + entry.getKey() + ", " + entry.getValue() + ")\r\n");

			}

			writer.close();

		} catch (IOException e) {

			e.printStackTrace();
		}

	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void setRouter(StreamRouter router) {
		// Do nothing
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(myFields);
	}

	@Override
	public Fields getSchema() {
		return myFields;
	}

}
