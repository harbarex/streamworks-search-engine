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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.bolt.BoltDeclarer;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.distributed.MultiJobSenderBolt;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tasks.ITask;
import edu.upenn.cis.stormlite.tasks.SpoutTask;

/**
 * Use multiple threads to simulate a cluster of worker nodes.
 * Hooks to other nodes in a distributed environment.
 * 
 * A thread pool (the executor) executes runnable tasks. Each
 * task involves calling a nextTuple() or execute() method in
 * a spout or bolt, then routing its tuple to the router.
 * 
 * @author zives
 *
 */
public class MultiJobDistributedCluster implements Runnable {

    static Logger log = LogManager.getLogger(MultiJobDistributedCluster.class);

    static AtomicBoolean quit = new AtomicBoolean(false);

    ConcurrentHashMap<String, HashMap<String, List<IRichBolt>>> boltStreams = new ConcurrentHashMap<String, HashMap<String, List<IRichBolt>>>();
    ConcurrentHashMap<String, HashMap<String, List<IRichSpout>>> spoutStreams = new ConcurrentHashMap<String, HashMap<String, List<IRichSpout>>>();
    ConcurrentHashMap<String, HashMap<String, StreamRouter>> streams = new ConcurrentHashMap<String, HashMap<String, StreamRouter>>();

    // TopologyContext context;
    ConcurrentLinkedQueue<TopologyContext> contexts = new ConcurrentLinkedQueue<TopologyContext>();
    ConcurrentLinkedQueue<TopologyContext> contextsToDo = new ConcurrentLinkedQueue<TopologyContext>();

    ObjectMapper mapper = new ObjectMapper();

    // each executor deals with one job (context)
    private int numberOfExecutors = 5;
    ExecutorService executor = Executors.newFixedThreadPool(numberOfExecutors);

    /**
     * Sets up the topology, instantiating objects (executors)
     * on the local machine. (Needs to be run on each worker in
     * a distributed cluster.)
     * 
     * Does not start worker threads; that is in startTopology.
     * 
     * 
     * @param name
     * @param config
     * @param topo
     * @return
     * @throws ClassNotFoundException
     */
    public TopologyContext submitTopology(String name, Config config,
            Topology topo) throws ClassNotFoundException {

        TopologyContext context = new TopologyContext(topo, new ConcurrentLinkedQueue<ITask>(), config);

        createSpoutInstances(topo, config, context);

        createBoltInstances(topo, config, context);

        createRoutes(topo, config, context);

        // add
        contextsToDo.add(context);

        return context;
    }

    // public TopologyContext submitTopology(String name, Config config,
    // Topology topo) throws ClassNotFoundException {
    // theTopology = name;

    // context = new TopologyContext(topo, taskQueue);

    // boltStreams.clear();
    // spoutStreams.clear();
    // streams.clear();

    // createSpoutInstances(topo, config);

    // createBoltInstances(topo, config);

    // createRoutes(topo, config);

    // // scheduleSpouts();

    // return context;
    // }

    /**
     * For consecutive job to run on this cluster
     */
    public void startSchedule() {

        // scheduleSpouts();
        TopologyContext context = contextsToDo.poll();

        if (context != null) {

            contexts.add(context);

        }

    }

    /**
     * Starts the worker thread to process events, starting with the spouts.
     */
    public void startTopology() {
        // Put the run method in a background thread
        new Thread(this).start();

    }

    private class JobRunner implements Runnable {

        Queue<ITask> taskQueue;
        TopologyContext context;

        ExecutorService jobExecutor = Executors.newFixedThreadPool(1);

        public JobRunner(TopologyContext ctx) {

            taskQueue = ctx.getQueue();
            context = ctx;

        }

        public void run() {

            while (!context.state.equals(TopologyContext.STATE.IDLE)) {

                ITask task = taskQueue.poll();

                if (task == null) {

                    Thread.yield();

                } else {

                    log.debug("Task from Job " + context.getConfig().get("jobID") + ": " + task.toString());

                    jobExecutor.execute(task);

                }

            }

        }
    }

    /**
     * The main executor loop uses Java's ExecutorService to schedule tasks.
     */
    public void run() {

        while (!quit.get()) {

            TopologyContext context = contexts.poll();

            if (context == null) {

                Thread.yield();

            } else {

                log.debug("Executor gets a new job:  " + context.getConfig().get("jobID"));

                // schedule
                scheduleSpouts(context.getConfig().get("jobID"), context.getQueue());

                // init
                JobRunner jobRunner = new JobRunner(context);

                executor.execute(jobRunner);

            }

        }

        executor.shutdown();

    }

    // public void run() {
    // while (!quit.get()) {
    // ITask task = taskQueue.poll();
    // if (task == null)
    // Thread.yield();
    // else {
    // log.debug("Task: " + task.toString());
    // executor.execute(task);
    // }
    // }
    // executor.shutdown();
    // }

    /**
     * Allocate units of work in the task queue, for each spout
     */
    // private void scheduleSpouts() {
    // for (String key : spoutStreams.keySet())
    // for (IRichSpout spout : spoutStreams.get(key)) {
    // taskQueue.add(new SpoutTask(spout, taskQueue));
    // }
    // }
    private void scheduleSpouts(String jobID, Queue<ITask> queue) {
        for (String key : spoutStreams.get(jobID).keySet())
            for (IRichSpout spout : spoutStreams.get(jobID).get(key)) {
                queue.add(new SpoutTask(spout, queue));
            }
    }

    /**
     * For each spout in the topology, create multiple objects (according to the
     * parallelism)
     * 
     * @param topo Topology
     * @throws ClassNotFoundException
     */
    private void createSpoutInstances(Topology topo, Config config, TopologyContext context)
            throws ClassNotFoundException {

        HashMap<String, List<IRichSpout>> spouts = new HashMap<String, List<IRichSpout>>();

        for (String key : topo.getSpouts().keySet()) {
            StringIntPair spout = topo.getSpout(key);

            spouts.put(key, new ArrayList<IRichSpout>());
            for (int i = 0; i < spout.getRight(); i++)
                try {
                    IRichSpout newSpout = (IRichSpout) Class.forName(spout.getLeft()).newInstance();

                    SpoutOutputCollector collector = new SpoutOutputCollector(context);

                    newSpout.open(config, context, collector);
                    spouts.get(key).add(newSpout);
                    log.debug("Created a spout executor " + key + "/" + newSpout.getExecutorId() + " of type "
                            + spout.getLeft());
                } catch (InstantiationException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
        }

        String jobID = config.get("jobID");

        spoutStreams.put(jobID, spouts);

    }

    /**
     * For each bolt in the topology, create multiple objects (according to the
     * parallelism)
     * 
     * @param topo Topology
     * @throws ClassNotFoundException
     */
    private void createBoltInstances(Topology topo, Config config, TopologyContext context)
            throws ClassNotFoundException {

        HashMap<String, List<IRichBolt>> bolts = new HashMap<String, List<IRichBolt>>();

        for (String key : topo.getBolts().keySet()) {

            StringIntPair bolt = topo.getBolt(key);

            bolts.put(key, new ArrayList<IRichBolt>());
            int localExecutors = bolt.getRight();
            for (int i = 0; i < localExecutors; i++)
                try {
                    OutputCollector collector = new OutputCollector(context);

                    IRichBolt newBolt = (IRichBolt) Class.forName(bolt.getLeft()).newInstance();
                    newBolt.prepare(config, context, collector);
                    bolts.get(key).add(newBolt);
                    log.debug("Created a bolt executor " + key + "/" + newBolt.getExecutorId() + " of type "
                            + bolt.getLeft());
                } catch (InstantiationException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
        }

        String jobID = config.get("jobID");
        boltStreams.put(jobID, bolts);

    }

    /**
     * Link the output streams to input streams, ensuring that the right kinds
     * of grouping + routing are accomplished
     * 
     * @param topo
     * @param config
     */
    private void createRoutes(Topology topo, Config config, TopologyContext context) {
        // Add destination streams to the appropriate bolts

        HashMap<String, StreamRouter> localStreams = new HashMap<String, StreamRouter>();

        String jobID = config.get("jobID");

        for (String stream : topo.getBolts().keySet()) {

            BoltDeclarer decl = topo.getBoltDeclarer(stream);

            StreamRouter router = decl.getRouter();

            localStreams.put(stream, router);

            int count = boltStreams.get(jobID).get(stream).size();

            // Create a bolt for each remote worker, give it the same # of entries
            // as we had locally so round-robin and partitioning will be consistent
            int workerId = 0;
            for (String worker : WorkerHelper.getWorkers(config)) {

                // Create one sender bolt for each node aside from us!
                if (workerId++ != Integer.valueOf(config.get("workerIndex"))) {
                    MultiJobSenderBolt sender = new MultiJobSenderBolt(worker, stream, jobID);
                    sender.prepare(config, context, null);
                    for (int i = 0; i < count; i++) {
                        router.addRemoteBolt(sender);
                        log.debug("Adding a remote route from " + stream + " to " + worker);
                    }

                    // Create one local executor for each node for us!
                } else {
                    for (IRichBolt bolt : boltStreams.get(jobID).get(stream)) {
                        router.addBolt(bolt);
                        log.debug("Adding a route from " + decl.getStream() + " to " + bolt);
                    }
                }
            }

            if (topo.getBolts().containsKey(decl.getStream())) {
                for (IRichBolt bolt : boltStreams.get(jobID).get(decl.getStream())) {
                    bolt.setRouter(router);
                    bolt.declareOutputFields(router);
                }
            } else {
                for (IRichSpout spout : spoutStreams.get(jobID).get(decl.getStream())) {
                    spout.setRouter(router);
                    spout.declareOutputFields(router);
                }
            }

        }

        streams.put(jobID, localStreams);

    }

    /**
     * Remove spouts, bolts, streams of a job
     */
    public void clearJobStreams(String id) {

        boltStreams.get(id).clear();
        spoutStreams.get(id).clear();
        streams.get(id).clear();

        // remove id
        boltStreams.remove(id);
        spoutStreams.remove(id);
        streams.remove(id);

    }

    /**
     * For each bolt in the topology, clean up objects
     * 
     * @param topo Topology
     */
    private void closeBoltInstances() {
        for (Map<String, List<IRichBolt>> boltMap : boltStreams.values()) {

            for (List<IRichBolt> boltSet : boltMap.values()) {
                for (IRichBolt bolt : boltSet)
                    bolt.cleanup();
            }

        }
    }

    /**
     * For each spout in the topology, create multiple objects (according to the
     * parallelism)
     * 
     * @param topo Topology
     */
    private void closeSpoutInstances() {
        for (Map<String, List<IRichSpout>> spoutMap : spoutStreams.values()) {

            for (List<IRichSpout> spoutSet : spoutMap.values()) {
                for (IRichSpout spout : spoutSet)
                    spout.close();
            }

        }

    }

    /**
     * Shut down the cluster
     * 
     * @param string
     */
    public void killTopology(String string) {
        if (quit.getAndSet(true) == false) {
            while (!quit.get())
                Thread.yield();
        }
        // System.out.println(context.getMapOutputs() + " local map outputs and " +
        // context.getReduceOutputs() + " local reduce outputs.");

        // for (String key : context.getSendOutputs().keySet())
        // System.out.println("Sent " + context.getSendOutputs().get(key) + " to " +
        // key);
    }

    /**
     * Shut down the bolts and spouts
     */
    public void shutdown() {
        closeSpoutInstances();
        closeBoltInstances();

        System.out.println("Shutting down distributed cluster.");
    }

    // public StreamRouter getStreamRouter(String stream) {
    // return streams.get(stream);
    // }
    public StreamRouter getStreamRouter(String id, String stream) {
        return streams.get(id).get(stream);
    }

}
