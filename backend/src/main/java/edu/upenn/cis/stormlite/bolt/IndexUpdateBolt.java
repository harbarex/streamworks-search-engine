package edu.upenn.cis.stormlite.bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
import edu.upenn.cis455.mapreduce.worker.storage.IndexStorage;
import edu.upenn.cis455.mapreduce.worker.storage.DocumentTaskStorage;

import edu.upenn.cis455.mapreduce.worker.storage.StorageFactory;
import edu.upenn.cis455.mapreduce.worker.storage.WorkerStorage;
import edu.upenn.cis455.mapreduce.worker.storage.entities.WordHit;

/**
 * A trivial bolt that simply outputs its input stream to the
 * console
 * 
 * @author zives
 *
 */
public class IndexUpdateBolt implements IRichBolt {

	static Logger log = LogManager.getLogger(IndexUpdateBolt.class);

	Fields myFields = new Fields();

	private boolean done = false;

	/**
	 * To make it easier to debug: we have a unique ID for each
	 * instance of the PrintBolt, aka each "executor"
	 */
	String executorId = UUID.randomUUID().toString();

	/**
	 * target output directory (under worker's local storage)
	 */
	private String targetOutputDirectory;

	private WorkerStorage localDB;

	private IndexStorage indexDB;

	private DocumentTaskStorage taskDB; // to get URL

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

	@Override
	public void cleanup() {

	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {

		this.collector = collector;
		this.context = context;

		String localDirectory = stormConf.get("storageDirectory");

		String dbDirectory = stormConf.get("DBDirectory");

		localDB = StorageFactory.getDatabase(dbDirectory);

		String workerIndex = (String) stormConf.get("workerIndex");

		// For (docID, words, tf)
		localDB.registerTable(this.executorId, String.class, Integer.class);
		localDB.clearTable(this.executorId);

		if (workerIndex.equals("0")) {

			String indexStorageDirectory = stormConf.get("indexStorage") + "/lexicon";

			indexDB = StorageFactory.getIndexDatabase(indexStorageDirectory);

		} else {

			indexDB = null;

		}

		// task storage
		String inputDir = (String) stormConf.get("input");
		String targetInputDirectory = WorkerServer.configureLocalSubDirectory(localDirectory, inputDir, false);

		// a local DB database is to be loaded
		// local DB: targetInputDirectory
		taskDB = StorageFactory.getDocumentTaskDatabase(targetInputDirectory);

		// number of workers
		String[] workers = WorkerHelper.getWorkers(stormConf);
		int numWorkers = workers.length;

		// How many map in the topo
		int numReduces = Integer.parseInt(stormConf.get("reduceExecutors"));

		log.debug("(IndexUpdateBolt) Index: " + stormConf.get("workerIndex")
				+ " , wait for " + numWorkers + " * " + numReduces + " (n) EOS");

		votesForEos = new ConsensusTracker(numReduces * numWorkers);

	}

	public void updateWordMeta(String word, String[] wordMeta) {

		// wordMeta: [docID, tf, df, localhitID, fromMainDoc]
		int wordID = this.indexDB.getWordID(word);

		this.indexDB.updateLexicon(wordID, word, Integer.parseInt(wordMeta[1]), Integer.parseInt(wordMeta[2]), "short");

	}

	@Override
	public boolean execute(Tuple input) {

		if (done) {

			if (!input.isEndOfStream())
				throw new RuntimeException("We received data after we thought the indexer had ended!");

			// Already done!
			return false;
		}

		if (!input.isEndOfStream()) {

			if (indexDB != null) {

				this.context.setState(TopologyContext.STATE.INDEXING);

				// updoad
				String key = input.getStringByField("key");

				// output: [word, docID, tf, df, localhitID]
				String[] value = (String[]) input.getObjectByField("value");

				log.debug("(IndexUpdate) Processing " + input.toString() + " from " + input.getSourceExecutor());

				this.updateWordMeta(key, value);

			}

			return true;

		} else {

			// add EOS & check
			if (votesForEos.voteForEos(input.getSourceExecutor())) {

				if (indexDB != null) {

					log.debug("(IndexUpdateBolt) finished!");

					// debug
					this.indexDB.showIndexStorageStatistics(false);

					this.done = true;

				}

				// set state to IDLE
				this.context.setState(TopologyContext.STATE.IDLE);

				return false;

			}

		}

		return false;
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
