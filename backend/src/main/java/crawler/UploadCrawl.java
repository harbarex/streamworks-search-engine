package crawler;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import api.data.Document;
import api.data.Link;

/**
 * Uploads one or more local document databases to DynamoDB
 * @author Daniel
 *
 */
public class UploadCrawl {
    final static Logger logger = LogManager.getLogger(UploadCrawl.class);
    static String idStoragePath;
    static boolean doUploadDocs;
    static boolean doUploadLinks;
    static int maxDocBuffer;
    static int maxLinkBuffer;
    static int printUpdateFreq = 1000;
    static String docsDBName;
    static String linksDBName;
    static String idOutputFile;
    
    /**
     * Parses command line arguments and config file
     * @param args
     */
    public static void parseArgs(String[] args) {
        if (args.length < 3) {
            System.out.println("Args: (config path) (id storage path) (doc storage path 1) [(doc storage path 2) ...]");
            System.exit(1);
        }
        String configPath = args[0];
        idStoragePath = args[1];
        JSONObject config = FileUtils.readConfig(configPath);
        doUploadDocs = config.has("doUploadDocs") ? config.getBoolean("doUploadDocs") : false;
        doUploadLinks = config.has("doUploadLinks") ? config.getBoolean("doUploadLinks") : false;
        maxDocBuffer = config.has("maxDocBuffer") ? config.getInt("maxDocBuffer") : 25;
        maxLinkBuffer = config.has("maxLinkBuffer") ? config.getInt("maxLinkBuffer") : 25;
        docsDBName = config.has("docsDBName") ? config.getString("docsDBName") : "CrawledDocuments_V2";
        linksDBName = config.has("linksDBName") ? config.getString("linksDBName") : "CrawledLinks_V2";
        idOutputFile = config.has("idOutputFile") ? config.getString("idOutputFile") : "";
    }

    public static void main(String[] args) {
        org.apache.logging.log4j.core.config.Configurator.setLevel("crawler", Level.DEBUG);
        parseArgs(args);
        DynamoUploader uploader = null;
        Storage docStorage = null;
        DocIDStorage idStorage = null;
        try {
            uploader = new DynamoUploader(maxDocBuffer, maxLinkBuffer);
            idStorage = new DocIDStorage(idStoragePath);
            
            if (doUploadDocs) {
                for (int i = 2; i < args.length; i++) {
                    logger.info("Uploading doc folder: " + i);
                    docStorage = new Storage(args[i]);
                    uploadDocs(idStorage, docStorage, uploader);
                    docStorage.close();
                }
            }

            Map<String, Integer> idMap = new HashMap<>();
            idMap.putAll(idStorage.docIDs);
            
            
            if (doUploadLinks) {
                for (int i = 2; i < args.length; i++) {
                    logger.info("Uploading link folder: " + i);
                    docStorage = new Storage(args[i]);
                    uploadLinks(idStorage, docStorage, uploader, idMap);
                    docStorage.close();
                }
            }
            
            if (idOutputFile != null && !idOutputFile.equals("")) {
                outputFile(idMap);
            }
            
            
            docStorage = null;
            logger.debug("Num docs uploaded: " + uploader.numDocsUploaded);
            logger.debug("Num docs buffered: " + uploader.numDocsBuffered);
            logger.debug("Num links uploaded: " + uploader.numLinksUploaded);
            logger.debug("Num links buffered: " + uploader.numLinksBuffered);

        }
        finally {
            if (uploader != null) {
                uploader.close();
            }
            if (docStorage != null) {
                docStorage.close();
            }
            if (idStorage != null) {
                idStorage.close();
            }
        }
    }

    /**
     * Generates a text file with doc ids and corresponding urls (for inspection purposes)
     * @param idMap
     */
    private static void outputFile(Map<String, Integer> idMap) {
        List<Integer> ids = new ArrayList<>(idMap.values());
        Map<Integer, String> invMap = new HashMap<>();
        for (String s : idMap.keySet() ){
                invMap.put(idMap.get(s), s);
        }
        Collections.sort(ids);
        StringBuilder builder = new StringBuilder();
        for (Integer i : ids) {
            builder.append("\n" + i + "\t" + invMap.get(i));
        }
        try {
            FileWriter w = new FileWriter(idOutputFile);
            w.write(builder.toString());
            w.close();
        } catch (IOException e) {
            logger.debug("Unable to output file: " + e);
        }

    }

    /**
     * Iterates through the given local links database and queues the links to be uploaded to DynamoDB.
     * @param idStorage
     * @param docStorage
     * @param uploader
     */
    private static void uploadLinks(DocIDStorage idStorage, Storage docStorage, DynamoUploader uploader, Map<String, Integer> idMap) {
        Link link;
        Entry<String, BDBListWrapper> entry;
        ArrayList<Integer> fromDocIDs;
        
        
        
        Iterator<Entry<String, BDBListWrapper>> edgeIter = docStorage.getEdges();
        Set<String> uploaded = new HashSet<>();
        int counter = 0;
        Integer docId;
        String url;
        while (edgeIter.hasNext()) {
            entry = edgeIter.next();
            url = (new URLInfo(entry.getKey())).toString();
            if (uploaded.contains(url)) {
                logger.error("duplicate: " + url);
                continue;
            }
            uploaded.add(url);
            docId = idMap.get(url);
            if (docId == null) {
                continue;
            }
            fromDocIDs = new ArrayList<>();
            for (String s : entry.getValue().fromEdges) {
                url = (new URLInfo(s)).toString();
                if (idMap.containsKey(url)) {
                    fromDocIDs.add(idMap.get(url));
                }
            }
            link = Link.createLink(docId, fromDocIDs);
            uploader.addLinkToBuffer(link);
            counter++;
            if (counter % printUpdateFreq == 0) {
                logger.info("Link counter: " + counter);
            }
        }
        while (uploader.linkBuffer.size() != 0) {
            uploader.flushLinkBuffer();
        }
    }

    /**
     * Iterates through the given local docs database and queues the documents to be uploaded to
     *  DynamoDB, generating integer ids for the crawled documents along the way.
     * @param idStorage
     * @param docStorage
     * @param uploader
     */
    private static void uploadDocs(DocIDStorage idStorage, Storage docStorage, DynamoUploader uploader) {
        
        CrawlerDocument cDoc;
        Integer docId;
        Document doc;
        int counter = 0;
        
        Iterator<CrawlerDocument> iter = docStorage.getDocIterator();
        while (iter.hasNext()) {
            cDoc = iter.next();
            if (cDoc.getContent().length() > (385 * Math.pow(2, 10))) {
                logger.error(cDoc.getURL());
                logger.error(cDoc.getContent().length());
                continue;
            }
            docId = idStorage.getID(cDoc.getURL());
            if (docId != null) {
                //continue;
            }
            docId = idStorage.getOrGenerateID(cDoc.getURL());
            doc = Document.createDocument(docId, cDoc.getURL(), cDoc.getContentType(), 
                    cDoc.getContent(), cDoc.getLastCrawled());
            uploader.addDocToBuffer(doc);
            counter++;
            if (counter % printUpdateFreq == 0) {
                logger.info("Doc counter: " + counter);
            }
        }
        
        while (uploader.docBuffer.size() != 0) {
            uploader.flushDocBuffer();
        }
    }
}
