package edu.upenn.cis.stormlite.bolt;

import java.security.Key;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.sleepycat.collections.StoredSortedMap;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.StringIntPair;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.distributed.ConsensusTracker;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.worker.storage.StorageFactory;
import edu.upenn.cis455.mapreduce.worker.storage.WorkerStorage;
import edu.upenn.cis455.mapreduce.worker.storage.IndexStorage;
import edu.upenn.cis455.mapreduce.worker.storage.HitStorage;
import edu.upenn.cis455.mapreduce.worker.storage.entities.WordHit;

/**
 * A simple adapter that takes a MapReduce "Job" and calls the "reduce"
 * on a per-tuple basis
 * 
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class IndexReduceBolt implements IRichBolt {
	static Logger log = LogManager.getLogger(IndexReduceBolt.class);

	Job reduceJob;

	/**
	 * This object can help determine when we have
	 * reached enough votes for EOS
	 */
	ConsensusTracker votesForEos;

	/**
	 * To make it easier to debug: we have a unique ID for each
	 * instance of the WordCounter, aka each "executor"
	 */
	String executorId = UUID.randomUUID().toString();

	Fields schema = new Fields("key", "value");

	boolean sentEos = false;

	// set up a global counter for reducer to name the hit database
	static AtomicInteger nextHitID = null;
	private int localID;

	// these following two are specific to individual executor
	private HitStorage hitStorage;
	private IndexStorage indexDB;

	/**
	 * local storage DB (under the worker server)
	 */
	private WorkerStorage dbStorage;

	/**
	 * This is where we send our output stream
	 */
	private OutputCollector collector;

	private TopologyContext context;

	int neededVotesToComplete = 0;

	public IndexReduceBolt() {
	}

	/**
	 * Initialization, just saves the output stream destination
	 */
	@Override
	public void prepare(Map<String, String> stormConf,
			TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.context = context;

		if (!stormConf.containsKey("reduceClass"))

			throw new RuntimeException("Mapper class is not specified as a config option");

		else {

			String mapperClass = stormConf.get("reduceClass");

			try {

				reduceJob = (Job) Class.forName(mapperClass).newInstance();

			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {

				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException("Unable to instantiate the class " + mapperClass);

			}
		}

		if (!stormConf.containsKey("mapExecutors")) {
			throw new RuntimeException("Reducer class doesn't know how many map bolt executors");
		}

		String localDirectory = stormConf.get("storageDirectory");

		// fetch local storage location in DB
		// this is for temporary tasks in Reducer (keep large things in disk)
		String dbDirectory = stormConf.get("DBDirectory");
		dbStorage = StorageFactory.getDatabase(dbDirectory);

		// here, this bolt stores each hit with unique key [word, docID, tag, pos]
		dbStorage.registerTable(this.executorId, String[].class, String[].class);
		dbStorage.clearTable(this.executorId);

		String indexStorageDirectory = stormConf.get("indexStorage");

		// set up local hit storage & index storage (for DocWords)
		int hitNumber = Integer.parseInt(stormConf.get("workerIndex"))
				* Integer.parseInt(stormConf.get("reduceExecutors"));

		if (nextHitID == null) {
			// start from hitNumber
			nextHitID = new AtomicInteger(hitNumber);
		}

		localID = nextHitID.getAndIncrement();

		// init hit local
		hitStorage = StorageFactory.getHitDatabase(indexStorageDirectory + "/hit" + localID);

		// local index DB
		indexDB = StorageFactory.getIndexDatabase(indexStorageDirectory + "/docWord" + localID);

		// TODO: determine how many EOS votes needed and set up ConsensusTracker
		// (or however you want to handle consensus)

		String[] workers = WorkerHelper.getWorkers(stormConf);
		int numWorkers = workers.length;

		// How many map in the topo
		int numMaps = Integer.parseInt(stormConf.get("mapExecutors"));

		log.debug("(IndexReduceBolt) Index: " + stormConf.get("workerIndex")
				+ " , wait for " + numWorkers + " * " + numMaps + " (n) EOS");

		votesForEos = new ConsensusTracker(numMaps * numWorkers);

	}

	/**
	 * Creaet WordHit based on the value
	 * 
	 * @param word
	 * @param value : [String[]], (context, docID, tag, pos, cap)
	 * @return
	 */
	public WordHit getWordHit(String word, String[] value) {

		int docID = Integer.parseInt(value[1]);
		String context = value[0];
		String tag = value[2];
		int pos = Integer.parseInt(value[3]);
		int cap = Integer.parseInt(value[4]);

		return new WordHit(docID, word, tag, pos, cap, 12, context);

	}

	/**
	 * Convert list of features to WordHit objects.
	 * 
	 * @param word
	 * @param hits
	 * @return
	 */
	public ArrayList<WordHit> convertToWordHits(String word, ArrayList<String[]> hits) {

		ArrayList<WordHit> wordHits = new ArrayList<WordHit>();

		for (String[] value : hits) {

			wordHits.add(getWordHit(word, value));

		}

		return wordHits;

	}

	public String[] storeDocWordAndGetWordMeta(String word, HashMap<Integer, ArrayList<String[]>> hits) {

		// output: [word, df, localHitID]

		int df = hits.size();

		for (int docID : hits.keySet()) {

			// TODO: Save local DocWord here (ignore wordID)
			this.indexDB.updateDocWord(docID, word, hits.get(docID).size(), "short");

		}

		String[] wordMeta = { word, "" + df, "" + localID };

		return wordMeta;

	}

	/**
	 * Process a tuple received from the stream, buffering by key
	 * until we hit end of stream
	 */
	@Override
	public synchronized boolean execute(Tuple input) {

		if (sentEos) {

			if (!input.isEndOfStream())
				throw new RuntimeException("We received data after we thought the stream had ended!");

			// Already done!
			return false;

		} else if (input.isEndOfStream()) {

			log.debug("Processing EOS from " + input.getSourceExecutor());

			if (votesForEos.voteForEos(input.getSourceExecutor())) {

				this.context.setState(TopologyContext.STATE.REDUCING);

				StoredSortedMap storedMaps = this.dbStorage.retrieveWorkerMap(executorId);

				String currentWord = null;

				HashMap<Integer, ArrayList<String[]>> hits = new HashMap<Integer, ArrayList<String[]>>();

				// iterate through the keys without batchifying it
				for (Object obj : storedMaps.entrySet()) {

					Map.Entry<Object, Object> entry = (Map.Entry<Object, Object>) obj;

					String[] nextKey = (String[]) entry.getKey();

					String nextWord = nextKey[0];

					int docID = Integer.parseInt(nextKey[1]);

					if ((currentWord != null) && (!currentWord.equals(nextWord))) {

						this.context.incReduceInputs(currentWord);

						// store the hit in nodes (executors)
						for (int docId : hits.keySet()) {

							this.hitStorage.addWordDocHits(currentWord, docId,
									convertToWordHits(currentWord, hits.get(docId)), "short");

						}

						// done with previous
						// wordMeta in the format
						// => String[] => [docID, tf, df, localhitID, fromMainDoc]
						String[] wordMeta = storeDocWordAndGetWordMeta(currentWord, hits);

						reduceJob
								.reduce(currentWord, wordMeta, this.collector, getExecutorId());

						hits.clear();

						currentWord = nextWord;

					}

					else if (currentWord == null) {

						currentWord = nextWord;

					}

					// add
					if (!hits.containsKey(docID)) {

						hits.put(docID, new ArrayList<String[]>());

					}

					// add data
					String[] intermediateHit = (String[]) entry.getValue();
					hits.get(docID).add(intermediateHit);

					// hits.get(docID).add(this.dbStorage.getIntermediateWordHit(executorId, key));

				}

				// make sure the last items are done
				if (hits.size() > 0) {

					this.context.incReduceInputs(currentWord);

					// save to local hit
					for (int docId : hits.keySet()) {

						this.hitStorage.addWordDocHits(currentWord, docId,
								convertToWordHits(currentWord, hits.get(docId)), "short");
					}

					// done with previous
					// String[] => [docID, tf, df, localhitID, fromMainDoc]
					String[] wordMeta = storeDocWordAndGetWordMeta(currentWord, hits);

					// flush out
					reduceJob.reduce(currentWord, wordMeta, this.collector, getExecutorId());

				}

				// send end of stream
				this.collector.emitEndOfStream(getExecutorId());

				// EOS sent
				sentEos = true;

				return false;
			}

		} else {

			// TODO: collect the tuples by key into BerkeleyDB
			// (until EOS arrives, in the above condition)
			log.debug("Processing " + input.toString() + " from " + input.getSourceExecutor());

			// incoming key: word, value: (context, docID, tag, pos, cap)
			// temporary storage => key: [word, docID, tag, pos]
			String inputKey = input.getStringByField("key");
			String[] inputValue = (String[]) input.getObjectByField("value");

			// use string[] is a trick to make it sorted
			String[] intermediateKey = { input.getStringByField("key"), inputValue[1], inputValue[2], inputValue[3] };

			this.dbStorage.addIntermediateWordHit(executorId, intermediateKey, inputValue);

		}

		return true;

	}

	/**
	 * Shutdown, just frees memory
	 */
	@Override
	public void cleanup() {

		// TODO: clean local storage
		this.dbStorage.clearTable(getExecutorId());

	}

	/**
	 * Lets the downstream operators know our schema
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(schema);
	}

	/**
	 * Used for debug purposes, shows our exeuctor/operator's unique ID
	 */
	@Override
	public String getExecutorId() {
		return executorId;
	}

	/**
	 * Called during topology setup, sets the router to the next
	 * bolt
	 */
	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);
	}

	/**
	 * The fields (schema) of our output stream
	 */
	@Override
	public Fields getSchema() {
		return schema;
	}

}
