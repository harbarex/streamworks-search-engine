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
package edu.upenn.cis.stormlite.bolt;

import java.util.ArrayList;
import java.util.List;

import edu.upenn.cis.stormlite.IOutputCollector;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis455.mapreduce.Context;

/**
 * Simplified version of Storm output queues
 * 
 * @author zives
 *
 */
public class OutputCollector implements IOutputCollector, Context {
	List<StreamRouter> routers = new ArrayList<>();
	TopologyContext context;

	public OutputCollector(TopologyContext context) {
		this.context = context;
	}

	@Override
	public void setRouter(StreamRouter router) {
		this.routers.add(router);
	}

	/**
	 * Emits a tuple to the stream destination
	 * 
	 * @param tuple
	 */
	public synchronized void emit(List<Object> tuple, String sourceExecutor) {
		for (StreamRouter router : routers)
			router.execute(tuple, context, sourceExecutor);
	}

	public synchronized void emitEndOfStream(String sourceExecutor) {
		for (StreamRouter router : routers)

			router.executeEndOfStream(context, sourceExecutor);
	}

	public List<StreamRouter> getRouters() {
		return routers;
	}

	@Override
	public void write(String key, String value, String sourceExecutor) {

		List<Object> values = new ArrayList<>();
		values.add(key);
		values.add(value);
		emit(values, sourceExecutor);

		// custom for workerstate
		if (context.getState().equals(TopologyContext.STATE.MAPPING)) {

			context.incMapOutputs(key);

		} else if (context.getState().equals(TopologyContext.STATE.REDUCING)) {

			context.incReduceOutputs(key);

		}

	}

	@Override
	public void write(String key, String[] value, String sourceExecutor) {

		List<Object> values = new ArrayList<>();
		values.add(key);
		values.add(value);
		emit(values, sourceExecutor);

		// custom for workerstate
		if (context.getState().equals(TopologyContext.STATE.MAPPING)) {

			context.incMapOutputs(key);

		} else if (context.getState().equals(TopologyContext.STATE.REDUCING)) {

			context.incReduceOutputs(key);

		}

	}

}
