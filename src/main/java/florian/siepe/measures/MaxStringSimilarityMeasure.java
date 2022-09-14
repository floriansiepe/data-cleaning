package florian.siepe.measures;

import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;

public class MaxStringSimilarityMeasure extends SimilarityMeasure<String> {
    private final SimilarityMeasure<String>[] measures;

    public MaxStringSimilarityMeasure(SimilarityMeasure<String>... measures) {
        this.measures = measures;
    }

    @Override
    public double calculate(String first, String second) {
        var maxSim = 0.0d;
        for (SimilarityMeasure<String> measure : this.measures) {
            final var sim = measure.calculate(first, second);
            if (sim > maxSim) {
                maxSim = sim;
            }
        }
        return maxSim;
    }
}
