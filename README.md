# StreamWorks: A fully distributed Search Engine

This project was developed as the final project for the Spring 2022 cohort of CIS-555 at the University of Pennsylvania, in collaboration with [Chun-Fu Yeh](https://github.com/YehCF), Duy Duc Doan and Daniel Stekol.

## Features

### Crawler
Implemented by Daniel Stekol

- Distributed crawler using AWS SQS for message transmission
- Per-instance multi-threading to maximize throughput despite heavy network and disk IO
- Content filtering based on page size, keywords, and language metadata
- Optimization via caching of seen and emitted URLs, robots.txt files, and seen-content hashes
- Domain blacklisting for domains which frequently return HTTP errors
- Pre-emptive timeouts for slow servers to avoid bottlenecking
- Crawl-breadth control via limiting the number of URLs emitted by each page
- Periodic worker updates (for monitoring and coordination) and disk flushing (for robustness)

#### Crawler Files
- BDBListWrapper.java
- Crawler.java
- CrawlerDocument.java
- CrawlMaster.java
- CrawlStateTracker.java
- DocIDStorage.java
- DomainHandler.java
- DynamoUploader.java
- FileUtils.java
- HeaderInfo.java
- MessageHandler.java
- PrintCrawlStats.java
- PurgeQueues.java
- Storage.java
- UploadCrawl.java
- URLEmitter.java
- URLInfo.java (slightly modified version of CIS 455 template file)



### Indexer
Implemented by Chun-Fu Yeh

1. MapReduce Master & Workers: make use of the MapReduce on Stormlite from HW3 to index the html documents from the crawler.

   - Task Scheduler: assign the crawled documents (from DynamoDB) to the spout's input directory when there is new document.
   - Index Spout: parse html documents and retrieve target html tags (h, a, p).
   - Index Mapper: parse each sentence into word features (word hits).
   - Index Reducer: aggregate word hits based on word-documentID.
   - Index Updater: aggregate words, which are used as the lexicon.
   - Index Synchronizer: synchronize the local data with the remote MySQL database (RDS) & send tf-idf query if necessary.

2. Index Server: provide keyword search services to search engine
   
   - Provide the matched documents to the keyword, each document having the following scores: (i) tf-idf, (ii) tag score, (iii) proximity score.
   - (Extra Credit) provide the context for each matched document, showing where the keywords are (the keywords are bolded).
   - (Extra Credit) fault tolerance & data monitor: the index server can be launched by either using the local data or connecting to the distributed word hit storage. Each set of word hits is stored in an redundant manner to prevent from some of nodes shut down unexpectedly. Basically, the server retrieves the word hits from these distributed nodes via REST-style messages.

3. Related Files:

   - edu.upenn.cis.stormlite.bolt.IndexMapBolt.java
   - edu.upenn.cis.stormlite.bolt.IndexReduceBolt.java
   - edu.upenn.cis.stormlite.bolt.IndexUpdateBolt.java
   - edu.upenn.cis.stormlite.spout.IndexFileSpout.java
   - edu.upenn.cis455.mapreduce.scheduler.DocInfoParseScheduler.java
   - edu.upenn.cis455.mapreduce.scheduler.DocumentTaskScheduler.java
   - edu.upenn.cis455.mapreduce.scheduler.IndexSynchronizer.java
   - edu.upenn.cis455.mapreduce.master.IndexerMasterServer.java
   - edu.upenn.cis455.mapreduce.worker.WorkerServer.java
   - edu.upenn.cis455.job.ShortDocIndex.java
   - all the java files under folder `indexer`

### Page Rank
Implemented by Ankit Billa


1. Page Rank Server: Provides REST API interface for Search Engine, based on MasterServer from HW3.


   - Computed PageRank scores can be queried given a list of Document IDs.

   - Scores are stored in a local BerkeleyDB instance to minimize network I/O, hosted on the same EC2 instance.

   - Link graph of document IDs retrieved from DynamoDB using a wrapper Class (DynamoStorage).

    

2. Page Rank Algorithm: Available to be run by making a REST API call to "/ranker/run".


   - Implemented without MapReduce to simplify pipeline

   - Utilizes a map from documentID -> Outgoing link documentIDs to populate adjacency lists of graph, retrieved from Crawler populated DynamoDB instance.

   - Document IDs are mapped to indices of final score array based on order of retrieval, for ease of computation.

   - Convergence threshold (epsilon) is set to break out of the loop if the algorithm converges before MAX_ITERATIONS are reached.

   - A damping Factor with value 0.85 is used, and a random surfer is simulated by adding an artificial link to the final pagerank scaling.


3. Related Files:

   - pagerank.PageRank.java
   - pagerank.RankScoreDatabase.java
   - storage.DynamoStorage.java
    
    
### Search Engine and Web Interface
Implemented by Duy Duc Doan

#### User Interface

The following pages have been created on user interface:

1. Starting page: with a search box to enter search keyword and a dropdown to select what type of search to perform (Page/Video)
2. Page Search Result page: display search box and search results. If user is logged in as an admin, there is a toggle to enable/disable debug mode to view the component scores used when ranking the results. If user enters a wrongly spelt keyword compared to some common keyword, there will be suggestion.
3. Video Search Result page: display search query and video results
4. Trending page: list the most commonly searched keywords by all users using the system.
5. (Admin only) Settings - Score Calculation: Customize the weight of scores used in final calculation of weighted rank score
6. (Admin only) Settings - System Config: customize some values of system

#### Search Engine REST API

1. Search Endpoints: result search results for some query. The data flow will be: asking indexer for certain numbers of matched documents, then send those to page ranker for rank score (score from page rank will be cached) and combined the final score using weighted formula (including tf-idf, proximity, tag score, and pagerank score)
2. User authentication and authorization systems: Endpoints to handle log in, logout, register and user role.
3. Feature endpoints: used for customizing weights of different scores
4. Config endpoints: used for customizing system configuration (e.g. rank cache TTL)

#### Related Files:

1. Frontend:

    - index file with routes: index.tsx, App.tsx
    - api: account-api.ts, configs-api.ts, feature-api.ts, keyword-api.ts, search-api.ts
    - components: Navbar.tsx, PaginationSection.tsx, ScoreCalculation.tsx, SearchSection.tsx, SystemConfig.tsx
    - pages: Documents.tsx, Login.tsx, Register.tsx, Settings.tsx, Trending.tsx, Videos.tsx, Welcome.tsx
    - other hooks and util files

2. REST server:

    - api/data: EngineSearchResult.java, IndexerMatch.java, RankerResult.java (interface to communicate between components)
    - engine/database: EngineBdb.java, EngineBdbViews.java (rank score cache in BDB)
    - engine/entity: Data entity used in the server: DocRank.java, Feature.java, PopularKeyword.java, User.java
    - engine/filters: AdminFilter.java, TokenFilter.java
    - egnine/handlers: LoginHandler.java, PopularKeywordHandler.java, RegisterHandler.java, SearchHandler.java, SpellcheckHandler.java
    - engine/service: AuthService.java, ConfigService.java, FeatureService.java, KeywordCount.java, PerformanceService.java, RankCache.java, UserService.java
    - engine: SearchEngine.java (main start point)

## Extra Credits
1. Integrate search results from other web services: users can search for Youtube video using the same keywork
2. Simple spellcheck:

- A list of popular keywords are recorded along with the count (which are used in trending page to show popular keywords)
- User query to compared to most popular keywords and calculate the edit distance between words as long as it is within some certain bound to determine whether a word is correctly spelt

3. Display of context of words in search result: queries are hightlighted in bold in the search result
4. Fault Tolerance for word hits: the parsed word hits are stored in a distributed and redundant manner to prevent from unexpected nodes shutdown.

## Instructions on how to install and run the project
### 1. Frontend:
- Make sure you have `node` and `npm` installed
- Run ```npm install```
- If you want to use the Youtube video search function: create a .env in frontend folder with the following content
```REACT_APP_YOUTUBE_API_KEY=<YOUR_GOOGLE_API_KEY>```

### 2. Backend
- Make sure you have Java and Maven installed
- For search engine: 
+ Create a .env file in backend folder with the following info 
```
ENGINE_PORT=44444
INDEXER_SERVER=http://54.163.51.147:44455
RANKER_SERVER=http://3.95.239.131:44454
AUTH_SECRET=streamwork
INDEXER_MYSQL_DBNAME=testDB
INDEXER_MYSQL_USERNAME=cis555final
INDEXER_MYSQL_PASSWORD=cis555final
INDEXER_MYSQL_HOSTNAME=cis555final.cgnodil0htxh.us-east-1.rds.amazonaws.com
INDEXER_MYSQL_PORT=3306
INDEXER_SHORT_LEXICON=ShortLexicon
INDEXER_PLAIN_LEXICON=PlainLexicon
INDEXER_SHORT_DOCWORDS=ShortDocWords
INDEXER_PLAIN_DOCWORDS=PlainDocWords
INDEXER_DOCINFO=DocInfo
INDEXER_NUM_HIT_SHARDS=12
INDEXER_HIT_SHARD_0_IPs=[192.168.64.29:8001,192.168.64.29:8002]
INDEXER_HIT_SHARD_1_IPs=[192.168.64.29:8001,192.168.64.29:8002]
INDEXER_HIT_SHARD_2_IPs=[192.168.64.29:8001,192.168.64.29:8002]
INDEXER_HIT_SHARD_3_IPs=[192.168.64.29:8001,192.168.64.29:8002]
INDEXER_HIT_SHARD_4_IPs=[192.168.64.29:8001,192.168.64.29:8002]
INDEXER_HIT_SHARD_5_IPs=[192.168.64.29:8001,192.168.64.29:8002]
INDEXER_HIT_SHARD_6_IPs=[192.168.64.29:8002,192.168.64.29:8001]
INDEXER_HIT_SHARD_7_IPs=[192.168.64.29:8002,192.168.64.29:8001]
INDEXER_HIT_SHARD_8_IPs=[192.168.64.29:8002,192.168.64.29:8001]
INDEXER_HIT_SHARD_9_IPs=[192.168.64.29:8002,192.168.64.29:8001]
INDEXER_HIT_SHARD_10_IPs=[192.168.64.29:8002,192.168.64.29:8001]
INDEXER_HIT_SHARD_11_IPs=[192.168.64.29:8002,192.168.64.29:8001]
DOCUMENTS_TABLE_NAME=CrawledDocuments_V2
LINKS_TABLE_NAME=CrawledLinks_V2
ENGINE_MYSQL_DBNAME=streamwork
ENGINE_MYSQL_USERNAME=doadmin
ENGINE_MYSQL_PASSWORD=AVNS_Pd2oWeyBGoVYrag
ENGINE_MYSQL_HOSTNAME=db-mysql-streamwork-do-user-11479913-0.b.db.ondigitalocean.com
ENGINE_MYSQL_PORT=25060
```
+ Run ```mvn clean install exec:java```


### 3. Crawler

#### 3.1 Launching crawl

- Create a list of line-separated seed urls and save them in a file called seeds.txt

- Create a list of line-separated filtering keywords and save them in a file called keywords.txt

- Create a config file called config.txt with the following contents (in JSON format):
```
{
"storagePath": "storage",
"maxDocSizeBytes": 399360,
"updateFrequency": 500,
"threadPoolSize": 10,
"maxURLEmits": 20,
"maxMessageBufferSize": 1000,
"readTimeoutMillis": 1000,
"connectTimeoutMillis": 500,
"filterKeywordThreshold": 4,
"minProbability": 0.01,
"minBadDomainFails": 50,
"minBadDomainFailRatio": 0.75,
"crawlerName": "cis455crawler",
"queueName": "CrawlerURLQueue"
}
```
- On each of the crawler machines, run the following Maven command from the "backend" folder:
```
mvn clean install exec:java@crawler -Dexec.args="[worker index] [total workers] [docs to crawl] [path to config.txt] [path to seeds.txt] [path to keywords.txt]"
```

Here is an example for launching a worker with index 0, with 5 total workers, and crawling 50000 documents:
```
mvn clean install exec:java@crawler -Dexec.args="0 5 50000 crawler_config.txt real_seeds.txt keywords.txt"
```

- Note that for the code to run, you must have set up corresponding SQS queues on AWS. There must be as many queues as their are workers, each named with the prefix "CrawlerURLQueue" followed by the index of the queue (for example, for running 3 workers, you must have three queues named CrawlerURLQueue0, CrawlerURLQueue1, CrawlerURLQueue2). Additionally, you must have configured the local AWS client on each machine with an IAM profile which has permission to read and write message to/from each of the SQS queues.

##### Explanation of config.txt options:

- storagePath: path to where the local BerkeleyDB files will be stored

- maxDocSizeBytes: maximum size of pages that can be downloaded (larger pages will be ignored)

- updateFrequency: how many messages to wait before emitting a crawl update to other workers

- threadPoolSize: how many threads to initialize on the local crawl worker

- maxURLEmits: maximum number of outgoing links any page is allowed to emit to the queue (limiting this number encourages broader exploration). If set to a negative number, no limits are imposed on the number of emitted links.

- maxMessageBufferSize: maximum number of messages to download and buffer from SQS. When the buffer reaches this size, the coordinator thread waits for the worker threads to remove items from the local queue before downloading more messages.

- readTimeoutMillis: maximum number of milliseconds to wait for a server to finish sending data before timing out. Should be set to higher than connectTimeoutMillis.

- connectTimeoutMillis: maximum number of milliseconds to wait for a server to establish a connection before timing out. This option ensures that slow server responses do not act as bottlenecks for the crawler.

- filterKeywordThreshold: minimum number of keywords a page must contain in order to be guaranteed to be saved (used by probabilistic content filtering)

- minProbability: minimum probability that any page has of being saved, even if it contains no keywords (used by probabilistic content filtering)

- minBadDomainFails: minimum number of failed HTTP requests that must occur in order for a domain to potentially be blacklisted by the crawler.

- minBadDomainFailRatio: minimum ratio of failed HTTP requests to total HTTP requests that must be reached in order for a domain to potentially be blacklisted by the crawler. A domain is blacklisted if at least [minBadDomainFails] requests to that domain have failed, and the overall fraction of failed requests is at least [minBadDomainFailRatio]. Setting this option to a value greater than 1 will disable domain blacklisting.

- crawlerName: the name to attach in the user-agent header of HTTP requests sent to websites

- queueName: the prefix of the existing AWS SQS queues used for sending urls to crawler workers. For n workers, there should be n SQS queues set up with the names [queueName]0, [queueName]1, ..., [queueName][n] (for instance, for three workers with the queueName prefix "CrawlerURLQueue", the names of the three SQS queues should be CrawlerURLQueue0, CrawlerURLQueue1, CrawlerURLQueue2 )


#### 3.2 Consolidating and uploading crawl data

Once the crawler has completed, each of the crawler machines will have created a local Berkeley DB instance in the specified storage directory (in the example configuation above, this folder will be named "storage"). In order to assign integer ids to each document and upload the data to DynamoDB, you should:

- Use the "scp" command line utility to copy each of the storage folders onto a single machine:
```
scp -r [path to ssh key] [path to local BDB storage folder] [remote machine address]:[destination path on remote machine]
```
- Create a config file called upload_config.txt with the following contents (in JSON format):
```
{
"doUploadDocs": true,
"doUploadLinks": true,
"maxDocBuffer": 24,
"maxLinkBuffer": 24,
"docsDBName": "CrawledDocuments_V2",
"linksDBName": "CrawledLinks_V2",
"idOutputFile": "docIdURLs.txt"
}
```
- Initialize a folder called id_storage for storing the document ids.

- Run the following Maven command from the "backend" folder:
```
mvn clean install exec:java@crawlerupload -Dexec.args="[path to upload_config.txt] [path to id_storage folder] [path to node 0 storage folder] [[path to node 1 storage folder] ...]""
```
For example
```
mvn clean install exec:java@crawlerupload -Dexec.args="upload_config.txt id_storage node0/storage node1/storage"
```

##### Explanation of upload_config.txt options:

- doUploadDocs: whether to upload documents to the document table (should be true unless this has already been done previously)

- doUploadLinks: whether to upload links to the link table (should be true unless this has already been done previously)

- maxDocBuffer: maximum number of documents to upload in a BatchWriteRequest to DynamoDB (AWS imposes a limit of at most 25)

- maxLinkBuffer: maximum number of links to upload in a BatchWriteRequest to DynamoDB (AWS imposes a limit of at most 25)

- docsDBName: name of the DynamoDB table where documents should be stored (this table is used by the indexer)

- linksDBName: name of the DynamoDB table where links should be stored, in adjacency list format, with one record per document (this table is used by the page ranker)

- idOutputFile: the name of the file where the program should outputs a list of all document ids and corresponding URLs. This option should be set to "" if no output file is desired.

### 4. Indexer

#### 4.1 Index Server

- Run $```mvn clean install exec:java@indexer -Dexec.args="[port] [local lexicon directory] [local hit storage directory] [activate distributed word hit storage mode]"```
  
  - for example: 
  
    - $mvn clean install exec:java@indexer -Dexec.args="44455 ./deploy/lexicon ./deployHits false" 
  
      - this launches the index server with the local lexicon & local word hits
  
    - $mvn clean install exec:java@indexer -Dexec.args="44455 ./deploy/lexicon ./deployHits true"
  
      - this launches the index server, which will fetch the word hits from remote / distributed worker nodes

#### 4.2 Preparation for Indexer MapReduce

  - Run ```mvn clean install exec:java@getDocs -Dexec.args="[(sync url) start from docID x] [(sync url) end at docID] [(retrieve doc) start from docID] [(retrieve doc) end at docID] [local BerkeleyDB directory]```

    - note: the credentials must be set and the ./.env file should be set as mentioned in previous section for Backend.
  

#### 4.3 Indexer MapReduce Master & Worker


- Master: run $```mvn clean install exec:java@indexMaster -Dexec.args="[port] [index directory]```


  - By default, the master will run on port 40001 and the main storage directory for indices is in ./index/demo
  

- Worker: run $```mvn clean install exec:java@indexWorker -Dexec.args="[port] [master IP : port] [worker temporary storage directory]"```
  
  - By default, the worker will run on port 8001, and the storage directory would be in ./node/node1
  

- Job Details:
  
  - In this project, the job file is ```edu.upenn.cis455.mapreduce.job.ShortDocIndex.java``` and the number of map & number of reduce are set to 2. 

#### 4.4 Indexer Synchronization with Remote RDS

  - Run $```mvn clean install exec:java@syncIndex -Dexec.args="[index directory] [batch size] [update lexicon] [update doc-word meta] [update doc info] [update Tf-Idf]"```

    - For example, to update lexicon, run $mvn clean install exec:java@syncIndex -Dexec.args="./index/demo/lexicon 1000 true false false false"
      - This inserts the local lexicon into the remote RDS, which is specified in ./.env file.
  

### 5. Page Rank

#### 5.1 Page Rank Server

- Run $```mvn clean install exec:java@pagerank -Dexec.args="[port] [rankScore database directory]```
  
  - When the Server is started, the Link Graph is retrieved from AWS, using the ```LINKS_TABLE_NAME``` stored in .env
  - By default, the server will run on port ```44454``` and the main storage directory for the BerkeleyDB instance for PageRank scores is in ```./storage```

#### 5.2 Running Page Rank Algorithm

- Make an HTTP GET request to ```http://3.95.239.131:44454/ranker/run```
  
  - This starts the iterative process of computing the PageRank scores, based on the Link Graph retrieved when the Server is started.

  

