package edu.upenn.cis455.mapreduce.worker.storage.entities;

import java.io.Serializable;
import java.util.Date;

public class DocWord implements Serializable {

    // note DocWord is unique by docID,word

    // docID, word, tf, ntf (normalized term frequency)

    private int docID;
    private String word;
    private int wordHash;

    // term frequency and normalized term frequency
    private int tf;
    private float ntf = 0;

    /**
     * 
     * @param docID
     * @param word
     * @param tf
     * @param ntf   (deprecated) => let remote db to calculate
     */
    public DocWord(int docID, String word, int tf, float ntf) {

        // set fields
        this.docID = docID;
        this.word = word;
        this.wordHash = this.word.hashCode();
        this.tf = tf;
        this.ntf = ntf;

    }

    /**
     * 
     * @param docID
     * @param word
     * @param tf
     * 
     */
    public DocWord(int docID, String word, int tf) {

        // set fields
        this.docID = docID;
        this.word = word;
        this.wordHash = this.word.hashCode();
        this.tf = tf;

    }

    public int getDocID() {

        return this.docID;

    }

    public String getWord() {

        return this.word;

    }

    public int getTf() {

        return this.tf;

    }

    public void addWordCount(int cnt) {

        this.tf += cnt;

    }

    // TODO: add query string for sync

    public String toString() {

        // return (word.equals("\"") || (word.equals("\\")))
        // ? "(" + docID + "," + wordID + "," + "\"" + "\\" + word + "\"" + "," + tf +
        // "," + ntf + ")"
        // : "(" + docID + "," + wordID + "," + "\"" + word + "\"" + "," + tf + "," +
        // ntf + ")";

        String printWord = word;

        if (word.contains("\\")) {

            printWord = printWord.replace("\\", "\\\\");

        }

        if (word.contains("\"")) {

            printWord = printWord.replace("\"", "\\\"");
        }

        return "(" + docID + "," + "\"" + printWord + "\"" + "," + tf + "," + ntf + "," + wordHash + ")";

    }

}
