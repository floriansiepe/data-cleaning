package florian.siepe.measures;

import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Word2VecSimilarity extends SimilarityMeasure<String> {
    private static final Logger logger = LoggerFactory.getLogger(Word2VecSimilarity.class);
    private final Word2Vec word2Vec;

    public Word2VecSimilarity(Word2Vec word2Vec) {
        this.word2Vec = word2Vec;
    }

    @Override
    public double calculate(String s1, String s2) {
        Word2VecSimilarity.logger.debug("{} - {}", s1, s2);
        return this.word2Vec.similarity(s1, s2);
    }
}
