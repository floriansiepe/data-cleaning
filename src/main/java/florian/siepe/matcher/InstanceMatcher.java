package florian.siepe.matcher;

import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.HashedDataSet;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import florian.siepe.blocker.LocalitySensitiveHashingBlockingKeyGenerator;
import florian.siepe.control.io.KnowledgeIndex;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.MatchableTableRow;
import florian.siepe.entity.kb.WebTables;
import org.apache.commons.text.similarity.JaccardSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static java.util.stream.Collectors.groupingBy;

/*
Currently not working properly
 */
public class InstanceMatcher {
    private static final Logger logger = LoggerFactory.getLogger(InstanceMatcher.class);
    private final HashMap<Integer, Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>>> correspondencesMap;
    private final KnowledgeIndex index;
    private final WebTables webTables;

    public InstanceMatcher(final HashMap<Integer, Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>>> correspondencesMap, KnowledgeIndex index, WebTables webTables) {
        this.correspondencesMap = correspondencesMap;
        this.index = index;
        this.webTables = webTables;
    }

    public void runMatching() {
        for (final var entry : correspondencesMap.entrySet()) {
            final var webTableId = entry.getKey();
            final var correspondences = entry.getValue();
            logger.info("Correspondences for {}", webTables.getTableNames().get(webTableId));
            for (final var cor : correspondences.get()) {
                runMatching(cor);
            }

        }
    }

    private void runMatching(final Correspondence<MatchableTableColumn, MatchableTableColumn> cor) {


        final var firstRecords = findIndexRecords(cor.getFirstRecord());
        final var secondRecords = findWebRecords(cor.getSecondRecord());
        final var firstDataSet = new HashedDataSet<MatchableTableRow, MatchableTableColumn>(firstRecords);
        final var secondDataSet = new HashedDataSet<MatchableTableRow, MatchableTableColumn>(secondRecords);

        logger.info("Instance matching dataset sizes: {}, {}", firstDataSet.size(), secondDataSet.size());
        final var firstColumnIndex = cor.getFirstRecord().getColumnIndex();
        final var secondColumnIndex = cor.getSecondRecord().getColumnIndex();
        final var localitySensitiveHashingBlockingKeyGenerator = new LocalitySensitiveHashingBlockingKeyGenerator(webTables, index, new HashSet<>(firstRecords), new HashSet<>(secondRecords), firstColumnIndex, secondColumnIndex);

        final var bucketsFirst = firstRecords.stream().collect(groupingBy(localitySensitiveHashingBlockingKeyGenerator::getKey));
        final var bucketsSecond = firstRecords.stream().collect(groupingBy(localitySensitiveHashingBlockingKeyGenerator::getKey));

        final var jaccardSimilarity = new JaccardSimilarity();
        for (final Map.Entry<Integer, List<MatchableTableRow>> bucketFirst : bucketsFirst.entrySet()) {
            final var bucketSecond = bucketsSecond.get(bucketFirst.getKey());

            if (bucketSecond == null) {
                continue;
            }

            for (final MatchableTableRow rowSecond : bucketSecond) {
                for (final MatchableTableRow rowFirst : bucketFirst.getValue()) {
                    final var sim = jaccardSimilarity.apply(rowFirst.get(firstColumnIndex).toString(), rowSecond.get(secondColumnIndex).toString());
                    if (sim > 0.5d) {
                        System.out.println();
                    }
                }
            }
        }

       /* final var engine = new MatchingEngine<MatchableTableRow, MatchableTableColumn>();

        final var correspondences = engine.runInstanceBasedSchemaMatching(firstDataSet, secondDataSet, localitySensitiveHashingBlockingKeyGenerator, localitySensitiveHashingBlockingKeyGenerator, BlockingKeyIndexer.VectorCreationMethod.TFIDF, new VectorSpaceCosineSimilarity(), 0.1d);
        // logger.info("{} <-> {}: {}", cor.getFirstRecord().toString(), cor.getSecondRecord().toString(), cor.getSimilarityScore());*/
        /*for (var correspondence : correspondences.get()) {
            logger.info("{} <-> {}: {}", correspondence.getFirstRecord().toString(), correspondence.getSecondRecord().toString(), correspondence.getSimilarityScore());
        }*/
    }

    private Collection<MatchableTableRow> findIndexRecords(final MatchableTableColumn column) {
        return index.getRecords().where(input -> input.hasColumn(column.getColumnIndex()) && input.getTableId() == column.getTableId()).get();
    }

    private Collection<MatchableTableRow> findWebRecords(final MatchableTableColumn column) {
        return webTables.getRecords().where(input -> input.hasColumn(column.getColumnIndex()) && input.getTableId() == column.getTableId()).get();
    }


    /*public HashMap<Integer, Processable<Correspondence<MatchableTableRow, MatchableTableColumn>>> runMatching(KnowledgeIndex index, WebTables webTables) {
        // Use the table id as key
        final var correspondences = new HashMap<Integer, Processable<Correspondence<MatchableTableRow, MatchableTableColumn>>>();

        final var webTableColumnsByTableId = webTables.getSchema().get().stream().collect(groupingBy(MatchableTableColumn::getTableId));


        for (final var entry : webTableColumnsByTableId.entrySet()) {
            final var tableId = entry.getKey();
            final var columns = entry.getValue();
            correspondences.put(tableId, runMatching(index, columns));
        }
        return correspondences;
    }

    private Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> runMatching(final KnowledgeIndex index, final List<MatchableTableColumn> columns) {
        final var webTableColumnSet = new HashedDataSet<MatchableTableRow, MatchableTableColumn>(columns);
        return runMatching(index.getSchema(), webTableColumnSet, null);
    }


    public Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> runMatching(final DataSet<MatchableTableColumn, MatchableTableColumn> dataSet, final DataSet<MatchableTableColumn, MatchableTableColumn> dataSet1, final Processable<Correspondence<MatchableTableColumn, Matchable>> processable) {
        MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<>();


        /*
        // define a blocker that uses the attribute values to generate pairs
        var blocker = new ClassAndTypeBasedSchemaBlocker();


        // to calculate the similarity score, aggregate the pairs by counting
        // and normalise with the number of record in the smaller dataset
        // (= the maximum number of records that can match)
        var aggregator
                = new VotingAggregator<>(
                false,
                Math.min(dataSet.size(), dataSet1.size()),
                0.0);

        try {
            return engine.runInstanceBasedSchemaMatching(dataSet, dataSet1, null);
        } catch (Exception e) {
            return new ProcessableCollection<>();
        }
    }*/
}
