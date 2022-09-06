package florian.siepe.measures;

import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.dws.winter.webtables.WebTablesStringNormalizer;

public class WebStringNormalizingSimilarity extends SimilarityMeasure<String> {
    private final SimilarityMeasure<String> measure;

    public WebStringNormalizingSimilarity(final SimilarityMeasure<String> measure) {
        this.measure = measure;
    }

    @Override
    public double calculate(final String first, final String second) {
        if(first==null || second==null) {
            return 0.0;
        }

        String s1 = WebTablesStringNormalizer.normaliseValue(first, true) + "";
        String s2 = WebTablesStringNormalizer.normaliseValue(second, true) + "";

        return measure.calculate(s1, s2);
    }
}
