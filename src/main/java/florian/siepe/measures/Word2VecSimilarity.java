package florian.siepe.measures;

import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Word2VecSimilarity extends SimilarityMeasure<String> {
    private static final Logger logger = LoggerFactory.getLogger(Word2VecSimilarity.class);
    private final Word2Vec word2Vec;

    public Word2VecSimilarity(final Word2Vec word2Vec) {
        this.word2Vec = word2Vec;
    }

    @Override
    public double calculate(final String s1, final String s2) {
        logger.debug("{} - {}", s1, s2);
        return word2Vec.similarity(s1, s2);
    }
}
