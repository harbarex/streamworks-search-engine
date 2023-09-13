package pagerank;

import java.util.*;
import java.util.stream.*;

import java.util.Date;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import api.data.Document;
import api.data.Link;
import api.data.RankerResult;
import io.github.cdimascio.dotenv.Dotenv;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import spark.Spark;
import storage.DynamoStorage;

import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;

import static spark.Spark.*;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.regions.Regions;


public class PageRank {
	
	 static Logger log = LogManager.getLogger(PageRank.class);
	 
	 private final int port;
	 
	 static RankScoreDatabase BDB = null;
	 public static String storageDirectory;
	 
	 private static Dotenv dotenv = Dotenv.configure().load();
	 private static String linksTableName = dotenv.get("LINKS_TABLE_NAME");
	    
	 public static LinkedHashMap<Integer, Integer> docIDMap = new LinkedHashMap<Integer, Integer>();
	 public static HashMap<Integer, ArrayList<Integer>> path = new HashMap<Integer, ArrayList<Integer>>();
	 
	 public static double pagerank[];
	 private static double old_pagerank[];
	 
	 private static double initialPageRank;
	 private static double dampingFactor = 0.85;
	 private static double epsilon = 1e-12;
	 private static int nodes;
	 
	 private static int MAX_ITER = 1000;
	 
	 private static boolean shutdown = false;
	 
	 public PageRank(int port) {
		 
		 // Server Initialization
		 
		 this.port = port;
		 
		 Spark.port(this.port);
		 
		 log.info("  Running PageRank Server on port : " + this.port);
		 
		 initialize();
		 
		 // Routes
		 
		 Spark.get("/ranker/run", (req, res) -> {
			 
			 String numWebpages = req.queryParams("webpages");
			 
			 if (numWebpages != null) {
				 int numNodes = Integer.valueOf(numWebpages);
				 nodes = numNodes;
			 }
			 
			 Date date = new Date();
		     long startTime = date.getTime();
		      
			 PageRank.calculateScores(nodes);
			 
			 date = new Date();
		     long endTime = date.getTime();
		     
		     double timeTaken = (endTime - startTime)/1e3;
		     
		     log.info(" Time taken to run PageRank algorithm: " + timeTaken + " s");
		     
		     SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
		     Date currDate = new Date(System.currentTimeMillis());
		     
			 return "<html><head><h1>Page Rank Server</h1></head><body>Successfully finished running PageRank algorithm on: " + 
			 formatter.format(currDate) + " <br>Time Taken : " + timeTaken + " seconds </body></html>";
	    	});
		 
		 Spark.get("/ranker/feedback", (req, res) -> {
			 int docID	  = Integer.valueOf(req.queryParams("docid"));
			 int feedback = Integer.valueOf(req.queryParams("feedback"));
			 
			 res.status(501);
			 return "Not implemented yet!";
	    	});
		 
		 Spark.post("/ranker/rank", (req, res) -> {
			 
			 String docList = req.body();
			 
			 String[] arr = docList.split("\\[")[1].split("\\]")[0].split(",");
		      
		     ArrayList<Integer> list = new ArrayList<Integer>();
		     
		     for (String  s: arr) {
		    	 list.add(Integer.valueOf(s.strip()));
		     }
		     
		     ArrayList<RankerResult> matches = new ArrayList<RankerResult>();
		     
		     Map<Integer, Double> scoreMap = BDB.getScoreMap();
		     
		     for (int id : list) {
		    	 int mappedID = docIDMap.get(id);
		    	 matches.add(new RankerResult(id, scoreMap.get(mappedID).floatValue()));
		     }
		     
			 String body = RankerResult.serialize(matches);
		     res.body(body);
		     
			 return body;
	         
	         });
		 
		 Spark.get("/ranker/shutdown", (req, res) -> {
			 
			 Spark.stop();
			 BDB.close();
			 return "Shutting down PageRank server!";
	    	});
		 

		 Spark.awaitInitialization();
		 
	 }
	 
	 
	 /**
	  * Initialize arrays for algorithm
	  */
	 private static void initialize() {
		 
		 initialPageRank = Double.valueOf(1) / Double.valueOf(nodes);
		 for(int i=1; i <= nodes; i++) {
			 if(!path.containsKey(i)) {
				 path.put(i, new ArrayList<Integer>());
			 }
		 }
	
		 pagerank = new double[nodes+1];
		 old_pagerank = new double[nodes+1];
	 }
	 
     /**
       * 
       * @param directory
       * 
       * If the given directory does not exist, create it
       */
     public static void directoryValidation(String directoryName) {
    	 File directory = new File(directoryName);
         if (! directory.exists()){
             directory.mkdirs();
         }
     }
     
     /**
      * Displays the URLs of n documents with 
      * highest pagerank scores
      * 
      * @param n
      */
     private static void getTopURLs(int n) {
    	 
    	 // Generate a Map of RankScore -> Array Index
    	 
    	 HashMap<Double, Integer> scoreMap = new HashMap<Double, Integer>();
    	 
    	 for(int j=1; j<=nodes; j++) {
    		 scoreMap.put(pagerank[j], j);
    	 }
    	 
    	 // Sort the Map based on RankScore
    	 
    	 List<Double> sortedScores = new ArrayList<>(scoreMap.keySet());
    	 Collections.sort(sortedScores, Collections.reverseOrder());
    	 
    	 // Get the top N Array Indices
    	 
    	 List<Integer> topNIndices = new ArrayList<Integer>();
    	 
    	 for(int k=0; k<n; k++) {
    		 topNIndices.add(scoreMap.get(sortedScores.get(k)));
    	 }
    	 
    	 
    	 // Get the corresponding top N docIDs
    	 
    	 List<Integer> topNDocIDs = new ArrayList<Integer>();
    	 
    	 for (Map.Entry<Integer, Integer> entry : docIDMap.entrySet()) {
    		    int docID 		= entry.getKey();
    		    int mappedID 	= entry.getValue();
    		    
    		    for(int idx : topNIndices) {
    		    	if(mappedID == idx) {
    		    		topNDocIDs.add(docID);
    		    		break;
    		    	}
    		    }
    	 }
    	 
    	 // Get the URL corresponding to the top N doc IDs
    	 
    	 File file = new File("./r3_docIdURLs.txt");
    	 
    	 BufferedReader br;
		 try {
		 	br = new BufferedReader(new FileReader(file));
		 	
		 	String line;
	   	    while ((line = br.readLine()) != null) {
	   	    	
	   	    	String id  = line.split("\\s+")[0];
	   	    	int docID  = Integer.valueOf(id);
	   	    	String url = line.split("\\s+")[1];
	   	    	
	   	    	if(topNDocIDs.contains(docID)) {
	   	    		int mappedID = docIDMap.get(docID);
	   	    		System.out.println("The document at: " + url + " has a pagerank score of: " + pagerank[mappedID]);
	   	    	}
	   	    	
	   	    }
	   	    
	   	    br.close();
	   	    
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	 
    	 
     }
     
     public static void printGraph() {
    	 
		  log.debug("\n Initial Graph \n");
		  for(Map.Entry element : path.entrySet()) {
			   int currentNode = (int) element.getKey();
			   ArrayList<Integer> destinationNodes = (ArrayList<Integer>) element.getValue();
			   log.debug(currentNode + " -> " + destinationNodes.toString());
			   
		   }
     }
     
     
     public static void printStats() {
    	 
    	 double [] finalPageRank = pagerank.clone();
		  Arrays.sort(finalPageRank);
		  
		  log.debug("\n Displaying bottom 10 PageRanks: \n");
		  
		  for(int j=1; j<=10; j++) {
			  log.debug(finalPageRank[j]);
		  }
		  
		  log.debug("\n Displaying top 10 PageRanks: \n");
		  
		  for(int j = (nodes - 10); j<=nodes; j++) {
			  log.debug(finalPageRank[j]);
		  }
		  
		  double sum = DoubleStream.of(finalPageRank).sum();
		  log.debug("\nThe sum of all pagerank values is : " + sum);
		  
		  getTopURLs(10);
    	 
     }
	 
	 public static void calculateScores(int totalNodes) {
		 
		  double OutgoingLinks = 0;
		  double TempPageRank[] = new double[totalNodes+1];
		  int k = 1; // For Traversing
		  int ITERATION_STEP = 1;
		  
		  log.info(" Running PageRank algorithm on " + totalNodes + " nodes.");
		  log.info(" Initial PageRank  of All Nodes : " + initialPageRank);
	
		  // Initialize all values with value 1/N, with index starting from 1
		  
		  for (k = 1; k <= totalNodes; k++) {
			  pagerank[k] = initialPageRank;
		  }
		  
		  // Simulates a random surfer to avoid sinks
		  double artificialLink = (1-dampingFactor)/(totalNodes);
	
		  while (ITERATION_STEP <= MAX_ITER) // Iterations
		  {
			   
			   // Store the PageRank for All Nodes in Temporary Array 
			   for (k = 1; k <= totalNodes; k++) {
			    TempPageRank[k] = pagerank[k];
			    old_pagerank[k]	= pagerank[k];
			    pagerank[k] = 0;
			   }
			   
			   for(Map.Entry element : path.entrySet()) {
				   int currentNode = (int) element.getKey();
				   ArrayList<Integer> destinationNodes = (ArrayList<Integer>) element.getValue();
				   OutgoingLinks = destinationNodes.size();
				   
				   for (int node : destinationNodes) {
					   pagerank[node] += TempPageRank[currentNode] * (Double.valueOf(1) / Double.valueOf(OutgoingLinks));
				   }
			   }
			   
			   // Add Damping Factor & Virtual Link to each PageRank value
			   
			   for (k = 1; k <= totalNodes; k++) {
				   pagerank[k] = artificialLink + (dampingFactor * pagerank[k]);
			   }
			   
			   // Check whether the algorithm has converged
			   
			   double error = 0.0;
			   
			   for (k = 1; k <= totalNodes; k++) {
				   error += Math.abs(old_pagerank[k] - pagerank[k]);
			   }
			   
			   error = error / Double.valueOf(totalNodes);
			   
			   if(error < epsilon) {
				   break;
			   }
		
			   ITERATION_STEP = ITERATION_STEP + 1;
		   
		  }
		  
		  log.info(" PageRank algorithm converged after " + ITERATION_STEP + " iterations.");
		  
		  // Display PageRank & add to local DB
		  for (k = 1; k <= totalNodes; k++) {
			  BDB.addTuple(k, pagerank[k]);
		  }

	 }

	 public static void retrieveCrawledLinks() {
		 
		 DynamoDbEnhancedClient enhancedClient = DynamoStorage.getDynamoDbEnhancedClient();
		 
		 
		 try {
			 
			 DynamoDbTable<Link> mappedTable = enhancedClient.table(linksTableName,
	                    TableSchema.fromBean(Link.class));
			 
			 Iterator<Link> results = mappedTable.scan().items().iterator();
			 int i = 1;
             while (results.hasNext()) {

                 Link link = results.next();
                 
                 if( link != null) {
                	 
                	 int docID = link.getDocId();
                	 
                	 if(!docIDMap.containsKey(docID)) {
                    	 docIDMap.put(docID, i++);
                     }
                     
                     ArrayList<Integer> filteredDocIDs = (ArrayList<Integer>) link.getToDocIds().stream().distinct()
    		                 .collect(Collectors.toCollection(ArrayList::new));
    				 
    				 ArrayList<Integer> toDocIDs = new ArrayList<Integer>();
    				 
    				 for(int outDocID : filteredDocIDs) {
    					 if(!docIDMap.containsKey(outDocID)) {
    						 docIDMap.put(outDocID, i++);
    					 }
    					 toDocIDs.add(docIDMap.get(outDocID));	 
    				 }
    				 
    				 path.put(docIDMap.get(docID), toDocIDs);
                 }
             }

	        } catch (DynamoDbException e) {

	            System.err.println(e.getMessage());
	            System.exit(1);

	     }
		 
		 nodes = docIDMap.size();
		 
		 log.info(" Successfully retrieved Link Graph with " + nodes + " nodes.");
		 
		 initialize();
		 
	 }
	 
	 private static void createDummyGraph1() {
		 nodes = 4;
		 initialize();
		 path.get(1).add(2);
		 path.get(1).add(3);
		 path.get(2).add(4);
		 path.get(3).add(1);
		 path.get(3).add(2);
		 path.get(3).add(4);
		 path.get(4).add(3);
		 
	 }
	 
	 private static void createDummyGraph2() {
		 nodes = 11;
		 initialize();
		 path.get(2).add(3);
		 path.get(3).add(2);
		 path.get(4).add(1);
		 path.get(4).add(2);
		 path.get(5).add(2);
		 path.get(5).add(4);
		 path.get(5).add(6);
		 path.get(6).add(2);
		 path.get(6).add(5);
		 path.get(7).add(2);
		 path.get(7).add(5);
		 path.get(8).add(2);
		 path.get(8).add(5);
		 path.get(9).add(2);
		 path.get(9).add(5);
		 path.get(10).add(5);
		 path.get(11).add(5);
		 
	 }
	 
	 public static void main(String args[]) {
		 
		 org.apache.logging.log4j.core.config.Configurator.setLevel("pagerank", Level.INFO);
		 
		 if (args.length != 1) {
	            System.out.println("Usage: Port Required: [port] [storage directory]");
	     }
		 
		 int port = 45555;
		 storageDirectory = "storage/";

	     if (args.length == 1) {
	    	 port = Integer.parseInt(args[0]);
	     }
	     
	     else if (args.length == 2){
	    	 port = Integer.parseInt(args[0]);
	    	 storageDirectory = args[1];
	     }
	     
	     directoryValidation(storageDirectory);
	     
	     // Create output BerkeleyDB instance for the page rank server
         try {
		 	BDB = new RankScoreDatabase(storageDirectory);
		 } catch (FileNotFoundException e) {
			 log.error("Unable to create BerkeleyDB for Page Rank Server");
			 e.printStackTrace();
		 }
         
         
		 PageRank p = new PageRank(port);
		 
		 //createDummyGraph1();
         
         //createDummyGraph2();
		 
		 retrieveCrawledLinks();
	     
		 if(shutdown) {
			 Spark.stop();
			 System.exit(0);
		 }
	
	 }
}
