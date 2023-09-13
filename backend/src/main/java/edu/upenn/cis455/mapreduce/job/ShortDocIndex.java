package edu.upenn.cis455.mapreduce.job;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.pipeline.*;

import indexer.nlp.Lemmatizer;

public class ShortDocIndex implements Job {

    private Lemmatizer lemmatizer = new Lemmatizer();

    public void map(String key, String value, Context context, String sourceExecutor) {

    }

    /**
     * Retrieve the context around the words
     * 
     * @param token
     * @param line  : [String], the line where the word is
     * @return
     */
    public String getContext(CoreLabel token, String line) {

        // limit to 150 characters around the work
        int loc = token.beginPosition();

        // generate window
        int begin = Math.max(loc - 50, 0);
        int end = Math.min(begin + 200, line.length());

        String context = line.substring(begin, end);

        // prepend "..." if necessary.
        StringBuilder builder = new StringBuilder();

        if (begin != 0) {

            builder.append(" ...");

        }

        builder.append(context);

        if (end != line.length()) {

            builder.append("... ");

        }

        return builder.toString();

    }

    public String[] getHitList(String[] value, CoreLabel token, int pos) {

        // value: [line, docID, tagName]
        // output: [context , docID, tag, pos, cap]
        String word = token.word();

        String cap = ((word.length() > 0) && (Character.isUpperCase(word.charAt(0)))) ? "1" : "0";

        String[] hit = { getContext(token, value[0]), value[1], value[2], "" + pos, cap };

        return hit;
    }

    /**
     * This is a method that lets us call map while recording the StormLite source
     * executor ID.
     * 
     */
    public void map(String key, String[] value, Context context, String sourceExecutor) {

        // value: [line, docID, tagName]
        // output
        // key : word (tokenized & lemmatized),
        // output: [context, docID, tag, pos, cap]

        List<CoreLabel> toks = lemmatizer.getLemmas(value[0]);

        for (int pos = 0; pos < toks.size(); pos++) {

            CoreLabel token = toks.get(pos);

            if (token.lemma().length() > 0) {

                String[] hit = getHitList(value, token, pos);

                // key: word, value: (context, docID, tag, pos, cap)
                context.write(token.lemma(), hit, sourceExecutor);

            }

        }
    }

    /**
     * This is a method that lets us call map while recording the StormLite source
     * executor ID.
     * 
     */
    public void reduce(String key, Iterator<String> values, Context context, String sourceExecutor) {

        int sum = 0;
        while (values.hasNext()) {
            sum += Integer.parseInt(values.next());
        }
        context.write(key, "" + sum, sourceExecutor);
    }

    public void reduce(String key, String[] values, Context context, String sourceExecutor) {

        context.write(key, values, sourceExecutor);

    }

}
