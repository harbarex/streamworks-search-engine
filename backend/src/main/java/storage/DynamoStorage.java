package storage;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;

import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;

import io.github.cdimascio.dotenv.Dotenv;

import java.util.ArrayList;
import java.util.List;

import api.data.Document;
import api.data.Link;

public class DynamoStorage {

    final static Logger logger = LogManager.getLogger(DynamoStorage.class);

    private static Dotenv dotenv = Dotenv.configure().load();
    private static String docsTableName = dotenv.get("DOCUMENTS_TABLE_NAME");
    private static String linksTableName = dotenv.get("LINKS_TABLE_NAME");

    /**
     * Get the DynamoDBClient.
     * Note:
     * Credentials & Region should be set up as the following instructions:
     * https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html.
     * 
     * @return
     */
    public static DynamoDbClient getDynamoDBClient() {

        Region region = Region.US_EAST_1;
        return DynamoDbClient.builder().region(region).build();

    }

    /**
     * Get the DynamoDbEnhancedClient for the convenient uploading & fetching object
     * in DynamoDB.
     * Note: No need to call getDynamoDBClient if getDynamoDbEnhancedClient is
     * called.
     * This is the one used to upload & retrieve item from DynamoDB
     * 
     * @return
     */
    public static DynamoDbEnhancedClient getDynamoDbEnhancedClient() {

        DynamoDbClient ddb = getDynamoDBClient();

        return DynamoDbEnhancedClient.builder().dynamoDbClient(ddb).build();

    }

    /**
     * Add a batch of documents using DynamoDbEnhancedClient
     * 
     * @param enhancedClient
     * @param docs           : [ArrayList<Document], see api/data/Document
     */
    public static void uploadBatchDocuments(DynamoDbEnhancedClient enhancedClient, ArrayList<Document> docs) {

        try {

            DynamoDbTable<Document> mappedTable = enhancedClient.table(docsTableName,
                    TableSchema.fromBean(Document.class));

            WriteBatch.Builder<Document> batchWritter = WriteBatch.builder(Document.class)
                    .mappedTableResource(mappedTable);

            for (Document doc : docs) {
                batchWritter.addPutItem(r -> r.item(doc));
            }

            // Create a BatchWriteItemEnhancedRequest object
            BatchWriteItemEnhancedRequest batchWriteItemEnhancedRequest = BatchWriteItemEnhancedRequest.builder()
                    .writeBatches(batchWritter.build())
                    .build();

            // Add these two items to the table
            enhancedClient.batchWriteItem(batchWriteItemEnhancedRequest);

            logger.debug("(DynamoDB Storage) Successfully upload batch = " + docs.size() + " (n) documents!");

        } catch (DynamoDbException e) {

            System.err.println(e.getMessage());
            System.exit(1);

        }
    }

    /**
     * Query a range of documents
     * 
     * @param enhancedClient
     * @param begin
     * @param end
     * @return
     */
    public static ArrayList<Document> queryDocuments(DynamoDbEnhancedClient enhancedClient, int begin, int end) {

        try {

            DynamoDbTable<Document> mappedTable = enhancedClient.table(docsTableName,
                    TableSchema.fromBean(Document.class));

            ArrayList<Document> retrievedDocs = new ArrayList<Document>();

            for (int i = begin; i < end; i++) {

                Key key = Key.builder().partitionValue(i).build();

                Document doc = mappedTable.getItem(r -> r.key(key));

                if (doc != null) {

                    retrievedDocs.add(doc);

                }

            }

            logger.debug("(DynamoStorage) Retrieve: " + retrievedDocs.size() + " (n) documents!");

            return retrievedDocs;

        } catch (DynamoDbException e) {

        }

        return null;

    }

    /**
     * Add a batch of Link objects using DynamoDbEnhancedClient
     * 
     * @param enhancedClient
     * @param docs
     */
    public static void uploadBatchLinks(DynamoDbEnhancedClient enhancedClient, List<Link> links) {

        try {

            DynamoDbTable<Link> mappedTable = enhancedClient.table(linksTableName,
                    TableSchema.fromBean(Link.class));

            WriteBatch.Builder<Link> batchWritter = WriteBatch.builder(Link.class)
                    .mappedTableResource(mappedTable);

            for (Link link : links) {
                batchWritter.addPutItem(r -> r.item(link));
            }

            // Create a BatchWriteItemEnhancedRequest object
            BatchWriteItemEnhancedRequest batchWriteItemEnhancedRequest = BatchWriteItemEnhancedRequest.builder()
                    .writeBatches(batchWritter.build())
                    .build();

            // Add these two items to the table
            enhancedClient.batchWriteItem(batchWriteItemEnhancedRequest);

            logger.debug("(DynamoDB Storage) Successfully upload batch = " + links.size() + " (n) Links!");

        } catch (DynamoDbException e) {

            System.err.println(e.getMessage());
            System.exit(1);

        }
    }

    /**
     * Query specific link
     * 
     * @param enhancedClient
     * @param begin
     * @param end
     * @return
     */
    public static Link queryLink(DynamoDbEnhancedClient enhancedClient, int sourceDocId) {

        try {

            DynamoDbTable<Link> mappedTable = enhancedClient.table(linksTableName,
                    TableSchema.fromBean(Link.class));

            Key key = Key.builder().partitionValue(sourceDocId).build();

            Link link = mappedTable.getItem(r -> r.key(key));

            logger.debug("(DynamoStorage) Retrieve: " + link.toString());

            return link;

        } catch (DynamoDbException e) {

        }

        return null;

    }

}
