package crawler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AddPermissionRequest;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;

/**
 * Utility script for purging SQS queues
 * @author Daniel
 *
 */
public class PurgeQueues {

    final static String QUEUE_NAME = "CrawlerURLQueue";
    
    public static void main(String[] args) {
        int numWorkers = Integer.valueOf(args[0]);
        AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
        String url;
        
        for (int i = 0; i < numWorkers; i++) {
            try {
                url = sqs.getQueueUrl(QUEUE_NAME + i).getQueueUrl();
                PurgeQueueRequest purgeReq = new PurgeQueueRequest().withQueueUrl(url);
                sqs.purgeQueue(purgeReq);
                System.out.println("Purged queue: " + i);
            }
            catch (QueueDoesNotExistException e) {

                System.out.println("Q does not exist");
            }
        }
        sqs.shutdown();

    }

}
