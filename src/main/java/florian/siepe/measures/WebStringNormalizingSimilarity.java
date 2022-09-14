package florian.siepe.measures;

import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.dws.winter.webtables.WebTablesStringNormalizer;

public class WebStringNormalizingSimilarity extends SimilarityMeasure<String> {
    private final SimilarityMeasure<String> measure;

    public WebStringNormalizingSimilarity(SimilarityMeasure<String> measure) {
        this.measure = measure;
    }

    @Override
    public double calculate(String first, String second) {
        if (null == first || null == second) {
            return 0.0;
        }

        final String s1 = WebTablesStringNormalizer.normaliseValue(first, true);
        final String s2 = WebTablesStringNormalizer.normaliseValue(second, true);

        return this.measure.calculate(s1, s2);
    }
}
