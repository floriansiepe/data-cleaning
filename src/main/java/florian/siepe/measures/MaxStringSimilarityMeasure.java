package florian.siepe.measures;

import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;

public class MaxStringSimilarityMeasure extends SimilarityMeasure<String> {
    private final SimilarityMeasure<String>[] measures;

    public MaxStringSimilarityMeasure(final SimilarityMeasure<String>... measures) {
        this.measures = measures;
    }

    @Override
    public double calculate(final String first, final String second) {
        var maxSim = 0d;
        for (final SimilarityMeasure<String> measure : measures) {
            var sim = measure.calculate(first, second);
            if (sim > maxSim) {
                maxSim = sim;
            }
        }
        return maxSim;
    }
}
