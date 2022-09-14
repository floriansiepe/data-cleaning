package florian.siepe.matcher;

import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.HashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.similarity.string.TokenizingJaccardSimilarity;
import florian.siepe.comparators.MaxMatchingRule;
import florian.siepe.comparators.TableColumnComparator;
import florian.siepe.control.io.KnowledgeIndex;
import florian.siepe.control.nlp.Word2VecFactory;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.WebTables;
import florian.siepe.measures.Word2VecSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

import static java.util.stream.Collectors.groupingBy;

public class PropertyMatcher {
    private static final Logger logger = LoggerFactory.getLogger(PropertyMatcher.class);
    private final double threshold;
    private final Word2VecFactory word2VecFactory;

    public PropertyMatcher(final boolean useWord2Vec, double threshold) {
        // In dev profile use more lightweight function
        this.word2VecFactory = useWord2Vec ? Word2VecFactory.getInstance() : null;
        this.threshold = threshold;
    }


    public Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> runMatching(DataSet<MatchableTableColumn, MatchableTableColumn> dataSet, DataSet<MatchableTableColumn, MatchableTableColumn> dataSet1, Processable<Correspondence<MatchableTableColumn, Matchable>> processable) {
        final MatchingEngine<MatchableTableColumn, MatchableTableColumn> engine = new MatchingEngine<>();

        var jaccardComparator = new TableColumnComparator(new TokenizingJaccardSimilarity());
        //final var matchingRule = new LinearCombinationMatchingRule<MatchableTableColumn, MatchableTableColumn>(0.6);
        var matchingRule = new MaxMatchingRule<MatchableTableColumn, MatchableTableColumn>(0.5);

        if (null != word2VecFactory) {
            var word2VecComparator = new TableColumnComparator(new Word2VecSimilarity(this.word2VecFactory.getWord2Vec()));
            matchingRule.addComparator(word2VecComparator);
        }

        matchingRule.addComparator(jaccardComparator);
        try {
            return engine.runLabelBasedSchemaMatching(dataSet, dataSet1, matchingRule, this.threshold);
        } catch (final Exception e) {
            return new ProcessableCollection<>();
        }
    }

    public HashMap<Integer, Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>>> runMatching(final KnowledgeIndex index, final WebTables webTables) {
        // Use the table id as key
        var correspondences = new HashMap<Integer, Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>>>();

        var webTableColumnsByTableId = webTables.getSchema().get().stream().collect(groupingBy(MatchableTableColumn::getTableId));

        for (var entry : webTableColumnsByTableId.entrySet()) {
            var webTableId = entry.getKey();
            var columns = entry.getValue();
            correspondences.put(webTableId, this.runMatching(index, columns));
        }
        return correspondences;
    }

    private Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> runMatching(KnowledgeIndex index, List<MatchableTableColumn> columns) {
        var webTableColumnSet = new HashedDataSet<MatchableTableColumn, MatchableTableColumn>(columns);
        return this.runMatching(index.getSchema(), webTableColumnSet, null);
    }
}
