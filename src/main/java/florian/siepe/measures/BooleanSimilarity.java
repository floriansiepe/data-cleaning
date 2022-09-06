package florian.siepe.measures;

import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;

public class BooleanSimilarity extends SimilarityMeasure<Boolean> {
    @Override
    public double calculate(final Boolean first, final Boolean second) {
        return first.equals(second) ? 1 : 0;
    }
}
