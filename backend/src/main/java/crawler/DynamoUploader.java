package crawler;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import api.data.Document;
import api.data.Link;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteResult;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

public class DynamoUploader {
    final static Logger logger = LogManager.getLogger(DynamoUploader.class);
    
    //String DOCS_DB_NAME = "CrawledDocuments_V2";
    //String LINKS_DB_NAME = "CrawledLinks_V2";
    
    int maxDocBuffer;
    int maxLinkBuffer;
    
    List<Document> docBuffer;
    List<Link> linkBuffer;
    DynamoDbClient client;
    DynamoDbEnhancedClient enhancedClient;
    public int numDocsUploaded;
    public int numDocsBuffered;
    public int numLinksUploaded;
    public int numLinksBuffered;
    
    public DynamoUploader(int maxDocBuffer, int maxLinkBuffer) {
        numDocsUploaded = 0;
        numDocsBuffered = 0;
        numLinksUploaded = 0;
        numLinksBuffered = 0;
        this.maxDocBuffer = maxDocBuffer;
        this.maxLinkBuffer = maxLinkBuffer;
        docBuffer = new ArrayList<>();
        linkBuffer = new ArrayList<>();
        client = DynamoDbClient.builder().region(Region.US_EAST_1).build();
        enhancedClient = DynamoDbEnhancedClient.builder().dynamoDbClient(client).build();
    }

    /**
     * Adds document to the buffer of docs waiting to be uploaded to database
     * @param doc
     */
    public void addDocToBuffer(Document doc) {
        docBuffer.add(doc);
        numDocsBuffered++;
        if (docBuffer.size() >= maxDocBuffer) {
            flushDocBuffer();
        }
    }
    
    /**
     * Flushes doc buffer to database. Buffer may still contain items afterward due
     * to DynamoDB throttling.
     */
    public void flushDocBuffer() {
        if (docBuffer.size() == 0) {
            return;
        }
        uploadBatchDocuments(docBuffer);
    }
    
    /**
     * Uploads the list of docs to DynamoDB as a batch write request. 
     * Any items that were not successfully processed are added back to the buffer,
     * whereas successfully processed items are removed.
     * @param docs
     */
    public void uploadBatchDocuments(List<Document> docs) {

        try {
            
            numDocsUploaded += docs.size();

            DynamoDbTable<Document> mappedTable = enhancedClient.table(UploadCrawl.docsDBName,
                    TableSchema.fromBean(Document.class));

            WriteBatch.Builder<Document> batchWritter = WriteBatch.builder(Document.class)
                    .mappedTableResource(mappedTable);

            for (Document doc : docs) {
                batchWritter.addPutItem(r -> r.item(doc));
            }

            BatchWriteItemEnhancedRequest batchWriteItemEnhancedRequest = BatchWriteItemEnhancedRequest.builder()
                    .writeBatches(batchWritter.build())
                    .build();

            BatchWriteResult res = enhancedClient.batchWriteItem(batchWriteItemEnhancedRequest);
            List<Document> unprocessed = res.unprocessedPutItemsForTable(mappedTable);
            docs.clear();
            docs.addAll(unprocessed);
            logger.debug("Doc batch write success. Items left in buffer: " + docs.size());

        }
         catch (DynamoDbException e) {
            logger.error("Cannot upload current doc batch to DynamoDB: ");
        }
    }
    
    /**
     * Adds link to the buffer of docs waiting to be uploaded to database
     * @param link
     */
    public void addLinkToBuffer(Link link) {
        linkBuffer.add(link);
        numLinksBuffered++;
        if (linkBuffer.size() >= maxLinkBuffer) {
            flushLinkBuffer();
        }
    }
    
    /**
     * Flushes link buffer to database. Buffer may still contain items afterward due
     * to DynamoDB throttling.
     */
    public void flushLinkBuffer() {
        if (linkBuffer.size() == 0) {
            return;
        }
        uploadBatchLinks(linkBuffer);
    }
    
    /**
     * Uploads the list of links to DynamoDB as a batch write request. 
     * Any items that were not successfully processed are added back to the buffer,
     * whereas successfully processed items are removed.
     * @param links
     */
    public void uploadBatchLinks(List<Link> links) {
        try {
            numLinksUploaded += links.size();
            DynamoDbTable<Link> mappedTable = enhancedClient.table(UploadCrawl.linksDBName,
                    TableSchema.fromBean(Link.class));

            WriteBatch.Builder<Link> batchWritter = WriteBatch.builder(Link.class)
                    .mappedTableResource(mappedTable);

            for (Link link : links) {
                batchWritter.addPutItem(r -> r.item(link));
            }

            BatchWriteItemEnhancedRequest batchWriteItemEnhancedRequest = BatchWriteItemEnhancedRequest.builder()
                    .writeBatches(batchWritter.build())
                    .build();

            BatchWriteResult res = enhancedClient.batchWriteItem(batchWriteItemEnhancedRequest);
            List<Link> unprocessed = res.unprocessedPutItemsForTable(mappedTable);
            links.clear();
            links.addAll(unprocessed);
            logger.debug("Link batch write success. Items left in buffer: " + links.size());

        } catch (DynamoDbException e) {
            logger.error("Cannot upload current link batch to DynamoDB:" + e);
            logger.info(links);
            throw e;
        }
    }
    
    /**
     * Closes the DynamoDB client
     */
    public void close() {
        client.close();
    }

}
