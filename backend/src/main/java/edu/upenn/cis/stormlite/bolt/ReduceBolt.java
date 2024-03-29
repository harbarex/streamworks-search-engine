package edu.upenn.cis.stormlite.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

public class ReduceBolt implements IRichBolt {
	static Logger log = LogManager.getLogger(ReduceBolt.class);

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

	/**
	 * Buffer for state, by key
	 */
	Map<String, List<String>> stateByKey = new HashMap<>();

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

	public ReduceBolt() {
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

		// TODO: fetch local storage location in DB
		String dbDirectory = stormConf.get("DBDirectory");

		dbStorage = StorageFactory.getDatabase(dbDirectory);

		dbStorage.registerTable(this.executorId, String.class, ArrayList.class);
		dbStorage.clearTable(this.executorId);

		// TODO: determine how many EOS votes needed and set up ConsensusTracker
		// (or however you want to handle consensus)

		String[] workers = WorkerHelper.getWorkers(stormConf);
		int numWorkers = workers.length;

		// How many map in the topo
		int numMaps = Integer.parseInt(stormConf.get("mapExecutors"));

		log.debug("(ReduceBolt) Index: " + stormConf.get("workerIndex")
				+ " , wait for " + numWorkers + " * " + numMaps + " (n) EOS");

		votesForEos = new ConsensusTracker(numMaps * numWorkers);

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

				// TODO: only if at EOS do we trigger the reduce operation
				// over what's in BerkeleyDB for the associated key, and output all state
				// You may find votesForEos useful to determine when consensus is reacked
				this.context.setState(TopologyContext.STATE.REDUCING);

				// iterate through the DB
				Set<String> storedKeys = this.dbStorage.getStoredKeys(getExecutorId());

				for (String key : storedKeys) {

					// record reduce read
					this.context.incReduceInputs(key);

					// reduce
					Iterator<String> iter = this.dbStorage.getValuesByKey(getExecutorId(), key).iterator();

					reduceJob.reduce(key, iter, this.collector, getExecutorId());

					// finish one reduce task
					this.context.incReduceOutputs(key);

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

			// Put k, v in to DB
			// path: dbStorage/nodeX dbName - executorID
			this.dbStorage.addKeyValuePair(this.executorId,
					input.getStringByField("key"),
					input.getStringByField("value"));

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
