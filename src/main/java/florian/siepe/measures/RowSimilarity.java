package florian.siepe.measures;

import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.dws.winter.similarity.date.NormalisedDateSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.numeric.NormalisedNumericSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.string.JaccardOnNGramsSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;
import florian.siepe.entity.kb.MatchableTableRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.function.BiFunction;

public class RowSimilarity extends SimilarityMeasure<MatchableTableRow> {
    private static final Logger logger = LoggerFactory.getLogger(RowSimilarity.class);
    private final int firstIndex;
    private final int secondIndex;
    private final BooleanSimilarity booleanSimilarity = new BooleanSimilarity();
    private final NormalisedNumericSimilarity normalisedNumericSimilarity = new NormalisedNumericSimilarity();
    private final NormalisedDateSimilarity dateSimilarity = new NormalisedDateSimilarity();

    private final SimilarityMeasure<String> stringSimilarity = new WebStringNormalizingSimilarity(new MaxStringSimilarityMeasure(new LevenshteinSimilarity(), new JaccardOnNGramsSimilarity(2)));
    private final BiFunction<Object, Object, Double> sim;

    public RowSimilarity(final int firstIndex, final int secondIndex, final DataType dataType) {
        this.firstIndex = firstIndex;
        this.secondIndex = secondIndex;
        this.sim = findSimilarityFunction(dataType);
    }

    private BiFunction<Object, Object, Double> findSimilarityFunction(final DataType dataType) {
        switch (dataType) {
            case bool:
                return (firstValue, secondValue) -> booleanSimilarity.calculate((Boolean) firstValue, (Boolean) secondValue);
            case date:
                return (firstValue, secondValue) -> dateSimilarity.calculate((LocalDateTime) firstValue, (LocalDateTime) secondValue);
            case numeric:
                return (firstValue, secondValue) -> normalisedNumericSimilarity.calculate((Double) firstValue, (Double) secondValue);
            default:
                return (firstValue, secondValue) -> stringSimilarity.calculate(firstValue.toString(), secondValue.toString());
        }
    }

    @Override
    public double calculate(final MatchableTableRow first, final MatchableTableRow second) {
        var firstValue = first.get(firstIndex);
        var secondValue = second.get(secondIndex);

        try {
            return sim.apply(firstValue, secondValue);
        } catch (ClassCastException e) {
            logger.warn("Cast exception: {}. Values {} and {}", e.getMessage(), firstValue, secondValue);
            return 0;
        }
    }
}
