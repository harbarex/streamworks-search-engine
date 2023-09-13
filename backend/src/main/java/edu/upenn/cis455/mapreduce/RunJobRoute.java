package edu.upenn.cis455.mapreduce;

import spark.Request;
import spark.Response;
import spark.Route;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.DistributedCluster;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;

public class RunJobRoute implements Route {
	static Logger log = LogManager.getLogger(RunJobRoute.class);
	DistributedCluster cluster;

	public RunJobRoute(DistributedCluster cluster) {
		this.cluster = cluster;
	}

	/**
	 * Single thread cluster
	 */
	private boolean alreadyRun = false;

	@Override
	public Object handle(Request request, Response response) throws Exception {

		log.info("(Worker) Starting job!");

		// TODO: start the topology on the DistributedCluster,
		// which should start the dataflow

		if (!alreadyRun) {

			// TODO: start the topology on the DistributedCluster,
			// which should start the dataflow
			alreadyRun = true;
			cluster.startSchedule();
			cluster.startTopology();

		} else {

			log.info("(Worker) Starting job with new schedule!");
			// no need to start a new thread, only need to put spout on the taskQueue
			cluster.startSchedule();

		}

		return "Started";
	}

}
