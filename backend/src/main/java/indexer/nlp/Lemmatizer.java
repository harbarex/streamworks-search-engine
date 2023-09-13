package indexer.nlp;

import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.pipeline.*;

public class Lemmatizer {

    private static Properties props = new Properties() {
        {
            setProperty("annotators", "tokenize,ssplit,pos,lemma");
        }
    };

    private StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

    public List<CoreLabel> getLemmas(String text) {

        // TODO: stop words & non-english characters

        CoreDocument doc = pipeline.processToCoreDocument(text);

        return doc.tokens();

    }
}
