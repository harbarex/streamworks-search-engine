package api.data;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

@DynamoDbBean
public class Document {

    //////////////////////////////
    //// Work as a DynamoDB entity
    //////////////////////////////
    private int docId;
    private String url; // starts with http://xx.xx.xx.xx:xx/ or https:xx.xx.xx.xx:xx//...
    private String content;
    private String contentType;
    private long visitedDate;

    /**
     * Use factory here to create a document object (DynamoDB way)
     * 
     */
    public static Document createDocument(int docId, String url, String contentType, String content,
            long visitedDate) {

        Document doc = new Document();

        doc.setDocId(docId);
        doc.setUrl(url);
        doc.setContentType(contentType);
        doc.setContent(content);
        doc.setVisitedDate(visitedDate);

        return doc;

    }

    @DynamoDbPartitionKey
    public int getDocId() {
        return docId;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public long getVisitedDate() {
        return visitedDate;
    }

    public void setVisitedDate(long visitedDate) {
        this.visitedDate = visitedDate;
    }

    public String toString() {

        return "docId: " + this.docId + " , url: " + this.url + ", type: " +
                this.contentType + " , LastTime: "
                + this.visitedDate;

    }

}
