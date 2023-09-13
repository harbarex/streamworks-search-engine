package crawler;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import api.data.Link;

/**
 * Utility script for iterating through the local document database and printing 
 * useful statistics such as the unique domains crawled.
 * @author Daniel
 *
 */
public class PrintCrawlStats {

    public static void main(String[] args) {
        String storageDir = args[0];
        Storage storage = null;
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(new FileWriter("docURLs.txt"));
            int totalCounter = 0;
            storage = new Storage(storageDir);
            Map<String, Integer> domainCounts = new HashMap<>();
            Iterator<CrawlerDocument> iter = storage.getDocIterator();
            CrawlerDocument doc;
            String domain;
            URLInfo urlInfo;
            int currentCount;
            int biggestLength = 0;
            while (iter.hasNext()) {
                
                totalCounter++;
                doc = iter.next();
                if (doc.getContent().length() > biggestLength) {
                    biggestLength = doc.getContent().length();
                }
                urlInfo = new URLInfo(doc.getURL());
                writer.println(urlInfo.toString());
                domain = urlInfo.getHostName();
                if (!domainCounts.containsKey(domain)) {
                    domainCounts.put(domain, 0);
                    //System.out.println(doc.toString() + "\n");
                }
                currentCount = domainCounts.get(domain);
                domainCounts.put(domain, currentCount + 1);
            }
            for (String d : domainCounts.keySet()) {
                System.out.println(d + ":\t\t\t " + domainCounts.get(d));
            }
            
            Iterator<Entry<String, BDBListWrapper>> edgeIter = storage.getEdges();
            Entry<String, BDBListWrapper> entry;
            int sumEdges = 0;
            int minEdges = 1000000;
            int maxEdges = 0;
            int currentNumEdges;
            int numEntries = 0;
            while (edgeIter.hasNext()) {
                numEntries++;
                entry = edgeIter.next();
                currentNumEdges = entry.getValue().fromEdges.size();
                sumEdges += currentNumEdges;
                minEdges = Math.min(minEdges, currentNumEdges);
                maxEdges = Math.max(maxEdges, currentNumEdges);
            }
            double avgEdges = ((double) sumEdges) / ((double) numEntries);
            
            System.out.println("minEdges: " + minEdges);
            System.out.println("maxEdges: " + maxEdges);
            System.out.println("avgEdges: " + avgEdges);
            
            System.out.println("biggestLength: " + biggestLength);
            System.out.println("totalCounter: " + totalCounter);
            System.out.println("Num unique domains: " + domainCounts.size());
        }
        catch (IOException e) {
            System.out.println("IOException writing to output file" + e);
        }
        finally {
            if (storage != null) {
                storage.close();
            }
            if (writer != null) {
                writer.close();
            }
        }
    }

}
