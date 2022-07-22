package florian.siepe.matcher;

import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.BlockingKeyIndexer;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.InstanceBasedSchemaBlocker;
import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.CSVRecordReader;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.blocking.DefaultAttributeValueGenerator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.similarity.vectorspace.VectorSpaceCosineSimilarity;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import florian.siepe.blocker.LocalitySensitiveHashingBlockingKeyGenerator;
import florian.siepe.control.io.KnowledgeIndex;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.MatchableTableRow;
import florian.siepe.entity.kb.WebTables;
import org.apache.commons.text.similarity.JaccardSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

/*
Currently not working properly
 */
public class InstanceMatcher  {
    private static final Logger logger = LoggerFactory.getLogger(InstanceMatcher.class);
    private final KnowledgeIndex index;
    private final WebTables webTables;

    public InstanceMatcher(KnowledgeIndex index, WebTables webTables) {
        this.index = index;
        this.webTables = webTables;
    }

    public void runMatching() {
        final var correspondences = new HashMap<Integer, Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>>>();
        final var webTableColumnsByTableId = webTables.getSchema().get().stream().collect(groupingBy(MatchableTableColumn::getTableId));

        for (final var entry : webTableColumnsByTableId.entrySet()) {
            final var webTableId = entry.getKey();
            final var webTableColumns = entry.getValue();
            //correspondences.put(webTableId, runMatching(webTableColumns, in));
            for (final Integer indexTableId : index.getTableIds().values()) {
                final var indexColumns = index.getPropertyIndices().get(indexTableId).values().stream().map(columnId -> index.findColumn(indexTableId, columnId)).collect(Collectors.toSet());
                runMatching(webTableColumns, indexColumns);
            }
        }

    }

    private void runMatching(final Collection<MatchableTableColumn> webTableColumns, final Collection<MatchableTableColumn> indexColumns) {
        for (final MatchableTableColumn webTableColumn : webTableColumns) {
            for (final MatchableTableColumn indexColumn : indexColumns) {
                // Do a quick null check since this can happen
                if (indexColumn != null && webTableColumn != null) {
                    runMatching(indexColumn, webTableColumn);
                }
            }
        }
    }

    private void runMatching(MatchableTableColumn indexColumn, MatchableTableColumn webColumn) {


        final var firstRecords = findIndexRecords(indexColumn);
        final var secondRecords = findWebRecords(webColumn);
        final var firstDataSet = new HashedDataSet<MatchableTableRow, MatchableTableColumn>(firstRecords);
        final var secondDataSet = new HashedDataSet<MatchableTableRow, MatchableTableColumn>(secondRecords);

        logger.info("Instance matching dataset sizes: {}, {}", firstDataSet.size(), secondDataSet.size());
        final var firstColumnIndex = indexColumn.getColumnIndex();
        final var secondColumnIndex = webColumn.getColumnIndex();
        final var localitySensitiveHashingBlockingKeyGenerator = new LocalitySensitiveHashingBlockingKeyGenerator(webTables, index, new HashSet<>(firstRecords), new HashSet<>(secondRecords), firstColumnIndex, secondColumnIndex);

        /*final var bucketsFirst = firstRecords.stream().collect(groupingBy(localitySensitiveHashingBlockingKeyGenerator::getKey));
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
        }*/


        /*DataSet<Record, Attribute> data1 = new HashedDataSet<>();
        new CSVRecordReader(-1).loadFromCSV(new File("usecase/movie/input/scifi1.csv"), data1);
        DataSet<Record, Attribute> data2 = new HashedDataSet<>();
        new CSVRecordReader(-1).loadFromCSV(new File("usecase/movie/input/scifi2.csv"), data2);
        InstanceBasedSchemaBlocker<Record, Attribute> blocker
                = new InstanceBasedSchemaBlocker<>(
                new DefaultAttributeValueGenerator(data1.getSchema()),
                new DefaultAttributeValueGenerator(data2.getSchema()));

        final var engine = new MatchingEngine<Record, Attribute>();
        engine.runInstanceBasedSchemaMatching(data1, data2, blocker, blocker, BlockingKeyIndexer.VectorCreationMethod.TFIDF, new VectorSpaceCosineSimilarity(), 0.1d);*/


        final var engine = new MatchingEngine<MatchableTableRow, MatchableTableColumn>();
        
        final var correspondences = engine.runInstanceBasedSchemaMatching(firstDataSet, secondDataSet, localitySensitiveHashingBlockingKeyGenerator, localitySensitiveHashingBlockingKeyGenerator, BlockingKeyIndexer.VectorCreationMethod.TFIDF, new VectorSpaceCosineSimilarity(), 0.1d);
        for (var correspondence : correspondences.get()) {
            logger.warn("{} <-> {}: {}", correspondence.getFirstRecord().toString(), correspondence.getSecondRecord().toString(), correspondence.getSimilarityScore());
        }
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
