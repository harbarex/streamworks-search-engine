package edu.upenn.cis.stormlite.distributed;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;

/**
 * This is a virtual bolt that is used to route data to the WorkerServer
 * on a different worker.
 * 
 * @author zives
 *
 */
public class SenderBolt implements IRichBolt {

	static Logger log = LogManager.getLogger(SenderBolt.class);

	// do batch sender
	private int batchSize = 100;
	private ArrayList<Tuple> batch = new ArrayList<Tuple>();

	/**
	 * To make it easier to debug: we have a unique ID for each
	 * instance of the WordCounter, aka each "executor"
	 */
	String executorId = UUID.randomUUID().toString();

	Fields schema = new Fields("key", "value");

	String stream;
	String address;
	ObjectMapper mapper = new ObjectMapper();
	URL url;
	URL batchUrl;

	TopologyContext context;

	boolean isEndOfStream = false;

	public SenderBolt(String address, String stream) {
		this.stream = stream;
		this.address = address;
	}

	/**
	 * Initialization, just saves the output stream destination
	 */
	@Override
	public void prepare(Map<String, String> stormConf,
			TopologyContext context, OutputCollector collector) {
		mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		this.context = context;
		try {
			url = new URL(address + "/pushdata/" + stream);
			batchUrl = new URL(address + "/pushbatch/" + stream);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException("Unable to create remote URL");
		}
	}

	/**
	 * Process a tuple received from the stream, incrementing our
	 * counter and outputting a result
	 */
	@Override
	public synchronized boolean execute(Tuple input) {
		try {
			send(input);
			// sendBatch(input);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}

	private void sendBatch(Tuple tuple) throws IOException {

		isEndOfStream = tuple.isEndOfStream();

		// not end of stream
		if (!isEndOfStream) {

			// add into the batch
			batch.add(tuple);

			// send when expected size of the batch is collected
			if (batch.size() == batchSize) {

				log.debug("Sender is routing the batch (n = " + batch.size() + ") from "
						+ batch.get(0).getSourceExecutor() + " to " + address
						+ "/" + stream);
				// send
				HttpURLConnection conn = (HttpURLConnection) batchUrl.openConnection();
				conn.setRequestProperty("Content-Type", "application/json");
				String jsonForBatch = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(batch);

				// Set to post and allow write
				conn.setRequestMethod("POST");
				conn.setDoOutput(true);

				// get & set byte length
				byte[] byteJson = jsonForBatch.getBytes();
				int contentLength = byteJson.length;
				conn.setFixedLengthStreamingMode(contentLength);

				// write
				OutputStream os = conn.getOutputStream();
				os.write(byteJson);
				os.flush();

				///////////
				conn.disconnect();

				// clear batch
				batch.clear();

			}

		} else {

			if (batch.size() > 0) {
				// add to the batch
				batch.add(tuple);

				// end of stream
				// => flush first

				log.debug("Sender is routing the last batch with EOS (n = " + batch.size() + ") from "
						+ batch.get(0).getSourceExecutor() + " to " + address
						+ "/" + stream);
				// send
				HttpURLConnection conn = (HttpURLConnection) batchUrl.openConnection();
				conn.setRequestProperty("Content-Type", "application/json");
				String jsonForBatch = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(batch);

				// Set to post and allow write
				conn.setRequestMethod("POST");
				conn.setDoOutput(true);

				// get & set byte length
				byte[] byteJson = jsonForBatch.getBytes();
				int contentLength = byteJson.length;
				conn.setFixedLengthStreamingMode(contentLength);

				// write
				OutputStream os = conn.getOutputStream();
				os.write(byteJson);
				os.flush();

				///////////
				conn.disconnect();

				// clear batch
				batch.clear();

			} else {

				// only EOS
				// try sleep a bit
				try {

					log.debug("(BatchSenderBolt) slightly delay EOS when sending it to other workers!");
					Thread.sleep(300);

				} catch (InterruptedException e) {

					e.printStackTrace();

				}

				ArrayList<Tuple> finalEos = new ArrayList<Tuple>();
				finalEos.add(tuple);

				// send
				HttpURLConnection conn = (HttpURLConnection) batchUrl.openConnection();
				conn.setRequestProperty("Content-Type", "application/json");
				String jsonForBatch = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(finalEos);

				// Set to post and allow write
				conn.setRequestMethod("POST");
				conn.setDoOutput(true);

				// get & set byte length
				byte[] byteJson = jsonForBatch.getBytes();
				int contentLength = byteJson.length;
				conn.setFixedLengthStreamingMode(contentLength);

				// write
				OutputStream os = conn.getOutputStream();
				os.write(byteJson);
				os.flush();

				///////////
				conn.disconnect();

			}

		}

	}

	/**
	 * Sends the data along a socket
	 * 
	 * @param stream
	 * @param tuple
	 * @throws IOException
	 */
	private void send(Tuple tuple) throws IOException {

		isEndOfStream = tuple.isEndOfStream();

		log.debug("Sender is routing " + tuple.toString() + " from " + tuple.getSourceExecutor() + " to " + address
				+ "/" + stream);

		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestProperty("Content-Type", "application/json");
		String jsonForTuple = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(tuple);

		// TODO: send this to /pushdata/{stream} as a POST!

		// TODO: try let sender bolt wait several ms for end of stream
		// (Currently, this is dealt in workerserver)
		if (isEndOfStream) {
			try {

				log.debug("(SenderBolt) slightly delay EOS when sending it to other workers!");
				Thread.sleep(100);

			} catch (InterruptedException e) {

				e.printStackTrace();

			}
		}

		// Set to post and allow write
		conn.setRequestMethod("POST");
		conn.setDoOutput(true);

		// get & set byte length
		byte[] byteJson = jsonForTuple.getBytes();
		int contentLength = byteJson.length;
		conn.setFixedLengthStreamingMode(contentLength);

		// write
		OutputStream os = conn.getOutputStream();
		os.write(byteJson);
		os.flush();

		///////////
		conn.disconnect();

	}

	/**
	 * Shutdown, just frees memory
	 */
	@Override
	public void cleanup() {
	}

	/**
	 * Lets the downstream operators know our schema
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(schema);
	}

	/**
	 * Used for debug purposes, shows our executor/operator's unique ID
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
		// NOP for this, since its destination is a socket
	}

	/**
	 * The fields (schema) of our output stream
	 */
	@Override
	public Fields getSchema() {
		return schema;
	}
}
