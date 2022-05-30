package florian.siepe.matcher;

import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.AbstractBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.Blocker;
import de.uni_mannheim.informatik.dws.winter.matching.rules.LinearCombinationMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.HashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.similarity.string.TokenizingJaccardSimilarity;
import florian.siepe.comparators.TableColumnComparator;
import florian.siepe.control.io.KnowledgeIndex;
import florian.siepe.control.nlp.Word2VecFactory;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.MatchableTableRow;
import florian.siepe.entity.kb.WebTables;
import florian.siepe.measures.Word2VecSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

import static java.util.stream.Collectors.groupingBy;

public class PropertyBlocker extends AbstractBlocker<MatchableTableColumn, MatchableTableColumn, MatchableTableRow>
        implements Blocker<MatchableTableColumn, MatchableTableColumn, MatchableTableColumn, MatchableTableColumn> {
    private static final Logger logger = LoggerFactory.getLogger(PropertyBlocker.class);
    private final Word2VecFactory word2VecFactory = Word2VecFactory.getInstance();

    public HashMap<Integer, Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>>> runBlocking(KnowledgeIndex index, WebTables webTables) {
        // Use the table id as key
        final var correspondencies = new HashMap<Integer, Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>>>();

        final var webTableColumnsByTableId = webTables.getSchema().get().stream().collect(groupingBy(MatchableTableColumn::getTableId));


        for (final var entry : webTableColumnsByTableId.entrySet()) {
            final var tableId = entry.getKey();
            final var columns = entry.getValue();
            correspondencies.put(tableId, runBlocking(index, columns));
        }
        return correspondencies;
    }

    private Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> runBlocking(final KnowledgeIndex index, final List<MatchableTableColumn> columns) {
        final var webTableColumnSet = new HashedDataSet<MatchableTableColumn, MatchableTableColumn>(columns);
        return runBlocking(index.getSchema(), webTableColumnSet, null);
    }


    @Override
    public Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> runBlocking(final DataSet<MatchableTableColumn, MatchableTableColumn> dataSet, final DataSet<MatchableTableColumn, MatchableTableColumn> dataSet1, final Processable<Correspondence<MatchableTableColumn, Matchable>> processable) {
        MatchingEngine<MatchableTableColumn, MatchableTableColumn> engine = new MatchingEngine<>();

        final var word2VecComparator = new TableColumnComparator(new Word2VecSimilarity(word2VecFactory.getWord2Vec()));
        final var jaccardComparator = new TableColumnComparator(new TokenizingJaccardSimilarity());
        final var linearMatchingRule = new LinearCombinationMatchingRule<MatchableTableColumn, MatchableTableColumn>(0.6);
        try {
            linearMatchingRule.addComparator(word2VecComparator, 0.6);
            linearMatchingRule.addComparator(jaccardComparator, 0.4);
        } catch (Exception e) {
            logger.error("This should never happen", e);
        }

        try {
            return engine.runLabelBasedSchemaMatching(dataSet, dataSet1, word2VecComparator, 0.5);
        } catch (Exception e) {
            return new ProcessableCollection<>();
        }
    }
}