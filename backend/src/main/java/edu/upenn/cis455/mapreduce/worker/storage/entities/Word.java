package edu.upenn.cis455.mapreduce.worker.storage.entities;

import java.io.Serializable;
import java.util.Date;

public class Word implements Serializable {

    // wordID, word, df, idf (just for padding)
    private int wordID;
    private String word;
    private int localPos;

    // document frequency, inverse document frequency
    private int df = 0;
    private float idf = 0;

    public Word(int wordID, String word, int df) {

        this.wordID = wordID;

        this.word = word;

        this.addDf(df);

    }

    public Word(int wordID, String word, int df, int localPos) {

        this.wordID = wordID;

        this.word = word;

        this.localPos = localPos;

        this.addDf(df);

    }

    public int getWordID() {

        return wordID;
    }

    public int getDf() {

        return df;

    }

    public int getLocalPos() {
        return localPos;
    }

    public void addDf(int newDf) {

        this.df += newDf;

    }

    // TODO: add query string for sync

    /**
     * TODO: handle special characters
     * (i) " (only ")
     * (ii) \ (only \)
     */
    public String toString() {

        // return (word.equals("\"") || (word.equals("\\")))
        // ? "(" + wordID + "," + "\"" + "\\" + word + "\"" + "," + df + "," + idf + ")"
        // : "(" + wordID + "," + "\"" + word + "\"" + "," + df + "," + idf + ")";
        String printWord = word;

        if (word.contains("\\")) {

            printWord = printWord.replace("\\", "\\\\");

        }

        if (word.contains("\"")) {

            printWord = printWord.replace("\"", "\\\"");
        }

        return "(" + wordID + "," + "\"" + printWord + "\"" + "," + df + "," + idf + "," + localPos + ")";

    }

}
