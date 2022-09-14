package florian.siepe.measures;

import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Function;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.dws.winter.similarity.string.JaccardOnNGramsSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;
import florian.siepe.entity.kb.MatchableTableRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.function.BiFunction;

public class RowSimilarity extends SimilarityMeasure<MatchableTableRow> {
    private static final Logger logger = LoggerFactory.getLogger(RowSimilarity.class);
    private final Function<Object, MatchableTableRow> recordProvider1;
    private final Function<Object, MatchableTableRow> recordProvider2;
    private final BooleanSimilarity booleanSimilarity = new BooleanSimilarity();
    private final EuclideanSimilarity euclideanSimilarity = new EuclideanSimilarity();
    private final NormalisedDateSimilarity dateSimilarity = new NormalisedDateSimilarity();

    private final SimilarityMeasure<String> stringSimilarity = new WebStringNormalizingSimilarity(new MaxStringSimilarityMeasure(new LevenshteinSimilarity(), new JaccardOnNGramsSimilarity(2)));
    private final BiFunction<Object, Object, Double> sim;

    public RowSimilarity(Function<Object, MatchableTableRow> recordProvider1, Function<Object, MatchableTableRow> recordProvider2, DataType dataType) {
        this.recordProvider1 = recordProvider1;
        this.recordProvider2 = recordProvider2;
        sim = this.findSimilarityFunction(dataType);
    }

    private BiFunction<Object, Object, Double> findSimilarityFunction(DataType dataType) {
        switch (dataType) {
            case bool:
                return (firstValue, secondValue) -> this.booleanSimilarity.calculate((Boolean) firstValue, (Boolean) secondValue);
            case date:
                return (firstValue, secondValue) -> {
                    final var first = LocalDateTime.parse(firstValue.toString());
                    final var second = LocalDateTime.parse(secondValue.toString());
                    int compare = first.compareTo(second);
                    this.dateSimilarity.setValueRange(0 > compare ? first : second, 0 > compare ? second : first);
                    return this.dateSimilarity.calculate((LocalDateTime) firstValue, (LocalDateTime) secondValue);
                };
            case numeric:
                return (firstValue, secondValue) -> this.euclideanSimilarity.calculate(Double.valueOf(firstValue.toString()), Double.valueOf(secondValue.toString()));
            case string:
                return (firstValue, secondValue) -> {
                    if ("null".equalsIgnoreCase(firstValue.toString()) || "null".equalsIgnoreCase(secondValue.toString())) {
                        return -1.0;
                    }
                    return this.stringSimilarity.calculate(firstValue.toString(), secondValue.toString());
                };
            default:
                return (firstValue, secondValue) -> this.stringSimilarity.calculate(firstValue.toString(), secondValue.toString());
        }
    }

    @Override
    public double calculate(MatchableTableRow first, MatchableTableRow second) {
        final var firstValue = this.recordProvider1.execute(first);
        final var secondValue = this.recordProvider2.execute(second);

        try {
            return this.sim.apply(firstValue, secondValue);
        } catch (final ClassCastException e) {
            //logger.error("Cast exception: {}. Values {} and {}", e.getMessage(), firstValue, secondValue);
            return 0;
        } catch (final Exception e) {
            //logger.error("Exception: {}. Values {} and {}", e.getMessage(), firstValue, secondValue);
            return 0;
        }
    }
}
