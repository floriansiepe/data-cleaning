package florian.siepe.measures;

import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;

public class BooleanSimilarity extends SimilarityMeasure<Boolean> {
    @Override
    public double calculate(Boolean first, Boolean second) {
        return first.equals(second) ? 1 : 0;
    }
}
