package florian.siepe.measures;

import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;

public class EuclideanSimilarity extends SimilarityMeasure<Double> {
    @Override
    public double calculate(Double first, Double second) {
        if (null == first || null == second) {
            return 0.0;
        }
        final var diff = Math.abs(first - second);
        if (1 > diff) {
            return 1.0;
        }
        return 1 / diff;
    }
}
