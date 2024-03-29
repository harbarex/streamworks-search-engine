/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.upenn.cis.stormlite;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tasks.ITask;

/**
 * Information about the execution of a topology, including
 * the stream routers
 * 
 * @author zives
 *
 */
public class TopologyContext {

	/**
	 * Added for multiple job functionality
	 */
	Config config;

	Topology topology;

	Queue<ITask> taskQueue;

	public static enum STATE {
		INIT, MAPPING, REDUCING, IDLE, INDEXING
	};

	STATE state = STATE.INIT;

	int mapOutputs = 0;

	// custom
	int mapInputs = 0;

	int reduceOutputs = 0;

	// custom
	int reduceInputs = 0;

	Map<String, Integer> sendOutputs = new HashMap<>();

	/**
	 * Mappings from stream IDs to routers
	 */
	Map<String, StreamRouter> next = new HashMap<>();

	public TopologyContext(Topology topo, Queue<ITask> theTaskQueue) {
		topology = topo;
		taskQueue = theTaskQueue;
	}

	/**
	 * Added for multiple job functionality
	 */
	public TopologyContext(Topology topo, Queue<ITask> theTaskQueue, Config conf) {
		topology = topo;
		taskQueue = theTaskQueue;
		config = conf;
	}

	public Topology getTopology() {
		return topology;
	}

	/**
	 * Added for multiple job functionality
	 */
	public Config getConfig() {

		return config;
	}

	/**
	 * Added for multiple job functionality
	 */
	public Queue<ITask> getQueue() {

		return taskQueue;
	}

	public void setTopology(Topology topo) {
		this.topology = topo;
	}

	public void addStreamTask(ITask next) {
		taskQueue.add(next);
	}

	public STATE getState() {
		return state;
	}

	public void setState(STATE state) {
		this.state = state;
	}

	public int getMapOutputs() {
		return mapOutputs;
	}

	public void incMapOutputs(String key) {
		this.mapOutputs++;
	}

	// custom for workerstate
	public int getMapInputs() {
		return mapInputs;
	}

	// custom for workerstate
	public void incMapInputs(String key) {
		this.mapInputs++;
	}

	public int getReduceOutputs() {
		return reduceOutputs;
	}

	public void incReduceOutputs(String key) {
		this.reduceOutputs++;
	}

	// custom for workerstate
	public int getReduceInputs() {
		return reduceInputs;
	}

	// custom for workerstate
	public void incReduceInputs(String key) {
		this.reduceInputs++;
	}

	public void incSendOutputs(String key) {
		if (!sendOutputs.containsKey(key))
			sendOutputs.put(key, Integer.valueOf(0));

		sendOutputs.put(key, Integer.valueOf(sendOutputs.get(key) + 1));
	}

	public Map<String, Integer> getSendOutputs() {
		return sendOutputs;
	}

}
