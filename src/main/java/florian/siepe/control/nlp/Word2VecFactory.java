package florian.siepe.control.nlp;

import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.util.Collection;

import static org.slf4j.LoggerFactory.getLogger;

public class Word2VecFactory extends SimilarityMeasure<String> implements Closeable {
    public static final String WORD_2_VEC_MODEL = "data/GoogleNews-vectors-negative300.bin";
    private static final Logger logger = getLogger(Word2VecFactory.class);
    private static Word2VecFactory service;
    private Word2Vec word2Vec;

    public static synchronized Word2VecFactory getInstance() {
        if (null != service) {
            return Word2VecFactory.service;
        }

        Word2VecFactory.service = new Word2VecFactory();
        Word2VecFactory.service.readModel();
        return Word2VecFactory.service;
    }

    public void readModel() {
        Word2VecFactory.logger.info("Loading Google's Word2Vec model... This may take some time");
        final File gModel = new File(Word2VecFactory.WORD_2_VEC_MODEL);
        this.word2Vec = WordVectorSerializer.readWord2VecModel(gModel);
    }

    public void closeModel() {
        if (null != word2Vec) {
            Word2VecFactory.logger.info("Closing Word2Vec model");
            this.word2Vec = null;
        }
    }

    @Override
    public double calculate(final String first, final String second) {
        if (null == word2Vec) {
            this.readModel();
        }

        return this.word2Vec.similarity(first, second);
    }

    public Collection<String> wordsNearest(final String str) {
        return this.wordsNearest(str, 10);
    }

    public Collection<String> wordsNearest(final String str, final int amount) {
        if (null == word2Vec) {
            this.readModel();
        }
        return this.word2Vec.wordsNearest(str, amount);
    }

    public Word2Vec getWord2Vec() {
        return this.word2Vec;
    }

    @Override
    public void close() {
        closeModel();
    }
}
