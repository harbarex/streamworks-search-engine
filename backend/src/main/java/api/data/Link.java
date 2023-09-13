package api.data;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

@DynamoDbBean
public class Link {

    //////////////////////////////
    //// Work as a DynamoDB entity
    //////////////////////////////
    private int docId;
    private List<Integer> toDocIds;

    /**
     * Use factory here to create a link object (DynamoDB way)
     * 
     */
    public static Link createLink(int sourceDocId, ArrayList<Integer> toDocIds) {

        Link link = new Link();

        link.setDocId(sourceDocId);
        link.setToDocIds(toDocIds);

        return link;

    }

    @DynamoDbPartitionKey
    public int getDocId() {
        return docId;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }

    public List<Integer> getToDocIds() {
        return this.toDocIds;
    }

    public void setToDocIds(List<Integer> toDocIds) {
        this.toDocIds = toDocIds;
    }

    public String toString() {

        return "Link (from " + docId + " to " + toDocIds.toString() + ")";
    }

}
