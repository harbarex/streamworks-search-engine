package edu.upenn.cis455.mapreduce.worker.storage.entities;

import java.io.Serializable;

public class WordHit implements Serializable {

    // [docID, word, tag, pos, cap, context]

    private int docID;
    private String word;
    private String tag;
    private int pos;
    private int cap;
    private int fontSize;
    private String context;

    /**
     * 
     * @param docID
     * @param word
     * @param tag
     * @param pos
     * @param cap
     * @param fontSize
     * @param context
     */
    public WordHit(int docID, String word, String tag, int pos, int cap, int fontSize, String context) {

        this.docID = docID;
        this.word = word;
        this.tag = tag;
        this.pos = pos;
        this.cap = cap;
        this.fontSize = fontSize;
        this.context = context;

    }

    public WordHit() {
        this.setDocID(0);
        this.setWord("");
        this.setTag("");
        this.setPos(0);
        this.setCap(0);
        this.setFontSize(0);
        this.setContext("");
    }

    public int getDocID() {

        return docID;

    }

    public void setDocID(int docID) {
        this.docID = docID;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public int getCap() {
        return cap;
    }

    public void setCap(int cap) {
        this.cap = cap;
    }

    public int getFontSize() {
        return fontSize;
    }

    public void setFontSize(int fontSize) {
        this.fontSize = fontSize;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    // TODO: add query string for sync

    public String toString() {

        // [docID, word, tag, pos, cap, fontSize, context]

        String printWord = word;

        if (word.contains("\\")) {

            printWord = printWord.replace("\\", "\\\\");

        }

        if (word.contains("\"")) {

            printWord = printWord.replace("\"", "\\\"");
        }

        String printContext = context;
        if (context.contains("\\")) {

            printContext = printContext.replace("\\", "\\\\");

        }

        if (context.contains("\"")) {

            printContext = printContext.replace("\"", "\\\"");
        }

        return "(" + docID + ","
                + "\"" + printWord + "\"" + ","
                + "\"" + tag + "\"" + ","
                + pos + ","
                + cap + ","
                + fontSize + ","
                + "\"" + printContext + "\"" + ")";

    }

}
