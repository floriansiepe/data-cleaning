package florian.siepe.control.nlp;

import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.util.Collection;

import static org.slf4j.LoggerFactory.getLogger;

public class Word2VecFactory implements Closeable {
    public static final String WORD_2_VEC_MODEL = "data/GoogleNews-vectors-negative300.bin";
    private static final Logger logger = getLogger(Word2VecFactory.class);
    private static Word2VecFactory service;
    private Word2Vec word2Vec;

    public synchronized static Word2VecFactory getInstance() {
        if (service != null) {
            return service;
        }

        service = new Word2VecFactory();
        service.readModel();
        return service;
    }

    public void readModel() {
        logger.info("Loading Google's Word2Vec model... This may take some time");
        File gModel = new File(WORD_2_VEC_MODEL);
        word2Vec = WordVectorSerializer.readWord2VecModel(gModel);
    }

    public void closeModel() {
        if (word2Vec != null) {
            logger.info("Closing Word2Vec model");
            word2Vec = null;
        }
    }

    public double similarity(String first, String second) {
        if (word2Vec == null) {
            readModel();
        }

        return word2Vec.similarity(first, second);
    }

    public Collection<String> wordsNearest(String str) {
        return wordsNearest(str, 10);
    }

    public Collection<String> wordsNearest(String str, int amount) {
        if (word2Vec == null) {
            readModel();
        }
        return word2Vec.wordsNearest(str, amount);
    }

    public Word2Vec getWord2Vec() {
        return word2Vec;
    }

    @Override
    public void close() {
        this.closeModel();
    }
}
