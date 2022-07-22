package florian.siepe.matcher;

import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.similarity.string.TokenizingJaccardSimilarity;
import florian.siepe.comparators.MaxMatchingRule;
import florian.siepe.comparators.TableColumnComparator;
import florian.siepe.control.nlp.Word2VecFactory;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.measures.Word2VecSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyMatcher extends AbstractMatcher {
    private static final Logger logger = LoggerFactory.getLogger(PropertyMatcher.class);
    private final double threshold;
    private final Word2VecFactory word2VecFactory;

    public PropertyMatcher(boolean useWord2Vec, final double threshold) {
        // In dev profile use more lightweight function
        word2VecFactory = useWord2Vec ? Word2VecFactory.getInstance() : null;
        this.threshold = threshold;
    }


    @Override
    public Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> runMatching(final DataSet<MatchableTableColumn, MatchableTableColumn> dataSet, final DataSet<MatchableTableColumn, MatchableTableColumn> dataSet1, final Processable<Correspondence<MatchableTableColumn, Matchable>> processable) {
        MatchingEngine<MatchableTableColumn, MatchableTableColumn> engine = new MatchingEngine<>();

        final var jaccardComparator = new TableColumnComparator(new TokenizingJaccardSimilarity());
        //final var matchingRule = new LinearCombinationMatchingRule<MatchableTableColumn, MatchableTableColumn>(0.6);
        final var matchingRule = new MaxMatchingRule<MatchableTableColumn, MatchableTableColumn>(0.5);

        if (word2VecFactory != null) {
            final var word2VecComparator = new TableColumnComparator(new Word2VecSimilarity(word2VecFactory.getWord2Vec()));
            matchingRule.addComparator(word2VecComparator);
        }

        matchingRule.addComparator(jaccardComparator);
        try {
            return engine.runLabelBasedSchemaMatching(dataSet, dataSet1, matchingRule, threshold);
        } catch (Exception e) {
            return new ProcessableCollection<>();
        }
    }
}
