package florian.siepe.matcher;

import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.HashedDataSet;
import de.uni_mannheim.informatik.dws.winter.processing.Function;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import florian.siepe.blocker.LocalitySensitiveHashingBlockingKeyGenerator;
import florian.siepe.blocker.TransformerBlocker;
import florian.siepe.blocker.TransformerRestClient;
import florian.siepe.control.SampleExtractor;
import florian.siepe.control.ValueExtractor;
import florian.siepe.control.io.KnowledgeIndex;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.MatchableTableRow;
import florian.siepe.entity.kb.WebTables;
import florian.siepe.measures.RowSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

/*
Currently not working properly
 */
public class InstanceMatcher {
    private static final Logger logger = LoggerFactory.getLogger(InstanceMatcher.class);
    private final KnowledgeIndex index;
    private final WebTables webTables;
    private final ValueExtractor valueExtractor;
    private final TransformerBlocker transformerBlocker;
    private final SampleExtractor sampleExtractor;

    public InstanceMatcher(final KnowledgeIndex index, final WebTables webTables, final TransformerRestClient transformerRestClient) {
        this.index = index;
        this.webTables = webTables;
        valueExtractor = new ValueExtractor(index, webTables);
        transformerBlocker = new TransformerBlocker(transformerRestClient, index, webTables);
        this.sampleExtractor = new SampleExtractor();
    }

    private static Map<Integer, List<MatchableTableRow>> generateBlocks(List<MatchableTableRow> records, LocalitySensitiveHashingBlockingKeyGenerator localitySensitiveHashingBlockingKeyGenerator) {
        return records
                .stream()
                .flatMap(record -> localitySensitiveHashingBlockingKeyGenerator.getKeys(record).map(blockId -> new AbstractMap.SimpleEntry<>(blockId, record)))
                .collect(groupingBy(AbstractMap.SimpleEntry::getKey))
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().stream().map(AbstractMap.SimpleEntry::getValue).collect(Collectors.toList())));
    }

    private static Map<Integer, List<MatchableTableRow>> generateBlock(List<MatchableTableRow> records, LocalitySensitiveHashingBlockingKeyGenerator localitySensitiveHashingBlockingKeyGenerator) {
        return records
                .stream()
                .collect(groupingBy(localitySensitiveHashingBlockingKeyGenerator::getKey));
    }

    public Map<Integer, Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>>> runMatching() {
        var correspondences = new HashMap<Integer, Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>>>();
        var webTableColumnsByTableId = this.webTables.getSchema().get().stream().collect(groupingBy(MatchableTableColumn::getTableId));

        for (var entry : webTableColumnsByTableId.entrySet()) {
            var tableCorrespondences = new ProcessableCollection<Correspondence<MatchableTableColumn, MatchableTableColumn>>();
            var webTableId = entry.getKey();
            var webTableColumns = entry.getValue();
            var blockedCandidates = this.transformerBlocker.generateCandidates(webTableColumns);
            for (var blockedPair : blockedCandidates.entrySet()) {
                var webColumn = blockedPair.getKey();
                var blockedIndexColumns = blockedPair.getValue();

                var correspondence = this.runMatching(blockedIndexColumns, webColumn);
                if (null != correspondence) {
                    tableCorrespondences.add(correspondence);
                }
            }
            //correspondences.put(webTableId, runMatching(webTableColumns, in));
            /*for (final Integer indexTableId : index.getTableIds().values()) {
                final var indexColumns = index.getPropertyIndices().get(indexTableId).values().stream().map(columnId -> index.findColumn(indexTableId, columnId)).collect(Collectors.toSet());
                runMatching(webTableColumns, indexColumns);
            }*/
            correspondences.put(webTableId, tableCorrespondences);
        }

        return correspondences;

    }

    private Correspondence<MatchableTableColumn, MatchableTableColumn> runMatching(Collection<MatchableTableColumn> indexColumns, MatchableTableColumn webTableColumn) {
        InstanceMatcher.logger.info("Run matching for {} with {} blocking candidates", webTableColumn.toString(), indexColumns.size());
        Correspondence<MatchableTableColumn, MatchableTableColumn> bestCorrespondence = null;

        for (MatchableTableColumn indexColumn : indexColumns) {
            // Do a quick null check since this can happen
            if (null != indexColumn && null != webTableColumn && indexColumn.getType() == webTableColumn.getType()) {
                var matching = this.runMatching(indexColumn, webTableColumn);
                if (null == bestCorrespondence) {
                    bestCorrespondence = matching;
                }
                if (bestCorrespondence.getSimilarityScore() < matching.getSimilarityScore()) {
                    bestCorrespondence = matching;
                }
            }
        }
        return bestCorrespondence;
    }

    private Correspondence<MatchableTableColumn, MatchableTableColumn> runMatching(final MatchableTableColumn indexColumn, final MatchableTableColumn webColumn) {
        InstanceMatcher.logger.debug("Start Matching");
        if (indexColumn.getType() != webColumn.getType()) {
            throw new RuntimeException("Data types are not matching: " + indexColumn.getType() + " and " + webColumn.getType());
        }

        List<MatchableTableRow> firstRecords = new ArrayList<>(this.valueExtractor.findIndexRecords(indexColumn));
        final List<MatchableTableRow> secondRecords = new ArrayList<>(this.valueExtractor.findWebRecords(webColumn));

        if (firstRecords.size() > Math.pow(secondRecords.size(), 2) || 5000 < firstRecords.size()) {
            // Record size is totally unbalanced. Hashing might take too long
            int maxRecords = Math.min((int) Math.pow(secondRecords.size(), 2), 5000);
            firstRecords = this.sampleExtractor.extractValue(maxRecords, firstRecords, t -> t);
        }

        InstanceMatcher.logger.debug("Finished values extraction");
        var firstDataSet = new HashedDataSet<MatchableTableRow, MatchableTableColumn>(firstRecords);
        firstDataSet.addAttribute(indexColumn);
        var secondDataSet = new HashedDataSet<MatchableTableRow, MatchableTableColumn>(secondRecords);
        secondDataSet.addAttribute(webColumn);

        InstanceMatcher.logger.debug("Instance matching dataset sizes: {}, {}", firstDataSet.size(), secondDataSet.size());

        // FINALLY I GOT ALL VALUES!!!!!

        final var indexValues = this.index.findColumnValues(indexColumn.getIdentifier());
        final var webtableValues = secondRecords.stream().map(rec -> rec.get(webColumn.getColumnIndex())).collect(Collectors.toList());

        MatchableTableColumn indexAttribute = this.index.findColumn(indexColumn.getIdentifier()).stream().findFirst().orElse(null);
        if (null == indexAttribute) {
            throw new RuntimeException("Could not find index attribute");
        }

        final Function<Object, MatchableTableRow> indexProvider = (rec) -> rec.get(rec.getPropertyUriToColumnIndex().get(indexAttribute.getIdentifier()));
        final Function<Object, MatchableTableRow> webProvider = (rec) -> rec.get(webColumn.getColumnIndex());

        InstanceMatcher.logger.debug("Start hashing");
        var localitySensitiveHashingBlockingKeyGenerator = new LocalitySensitiveHashingBlockingKeyGenerator(new HashSet<>(firstRecords), new HashSet<>(secondRecords), indexProvider, webProvider);

        var bucketsFirst = InstanceMatcher.generateBlocks(firstRecords, localitySensitiveHashingBlockingKeyGenerator);
        var bucketsSecond = InstanceMatcher.generateBlocks(secondRecords, localitySensitiveHashingBlockingKeyGenerator);
        bucketsFirst.remove(-1);
        bucketsSecond.remove(-1);
        InstanceMatcher.logger.debug("Finished hashing");


        var similarityMeasure = new RowSimilarity(indexProvider, webProvider, indexColumn.getType());
        double blockSimilaritySum = 0;
        int blockCount = 0;

        for (Map.Entry<Integer, List<MatchableTableRow>> bucketFirst : bucketsFirst.entrySet()) {
            if (bucketFirst.getValue().isEmpty()) {
                continue;
            }
            var bucketSecond = bucketsSecond.get(bucketFirst.getKey());

            if (null == bucketSecond || bucketSecond.isEmpty()) {
                continue;
            }

            final var similarities = new double[bucketSecond.size()][bucketFirst.getValue().size()];

            for (int i = 0; i < bucketSecond.size(); i++) {
                final var rowSecond = bucketSecond.get(i);

                for (int j = 0; j < bucketFirst.getValue().size(); j++) {
                    final var rowFirst = bucketFirst.getValue().get(j);
                    try {
                        var sim = similarityMeasure.calculate(rowFirst, rowSecond);
                        similarities[i][j] = sim;
                        if (0.6 < sim) {
                            InstanceMatcher.logger.debug("Similarity: {} between {} and {}", sim, indexProvider.execute(rowFirst), webProvider.execute(rowSecond));
                        }
                    } catch (final NullPointerException e) {
                        InstanceMatcher.logger.debug("Got a null value while trying to fetch value: {}", e.getMessage());
                    }
                }
            }

            //final double blockSimilarity = computeBlockSimilarity(similarities);
            double blockSimilarity = this.computeBlockSimilarityMax(similarities);
            if (0.2 < blockSimilarity) {
                blockSimilaritySum += blockSimilarity;
                blockCount++;
            }
        }
        final double averageSimilarity = blockSimilaritySum / blockCount;

        if (0.5d < averageSimilarity) {
            InstanceMatcher.logger.debug("Match: {} {} <-> {}", averageSimilarity, indexColumn, webColumn);
        }
        return new Correspondence<>(indexColumn, webColumn, averageSimilarity);
    }

    private double computeBlockSimilarityMax(double[][] similarities) {
        double max = 0;
        for (double[] similarity : similarities) {
            for (double v : similarity) {
                if (v > max) {
                    max = v;
                }
            }
        }
        return max;
    }

    private double computeBlockSimilarity(double[][] similarities) {
        // Formula is from https://dbs.uni-leipzig.de/file/BTW-Workshop_2007_EngmannMassmann.pdf 2.3 Content-based Matching
        final int frameHeight = similarities.length;
        if (0 == frameHeight) {
            return 0;
        }
        final int frameWidth = similarities[0].length;
        // Framecorrection is used when the sim function emitted -1 e.g. for null values
        int frameCorrection = 0;
        double maxLeftSide = 0;
        double maxRightSide = 0;

        for (int i = 0; i < frameHeight; i++) {
            double maxI = 0;
            for (int j = 0; j < frameWidth; j++) {
                double sim = similarities[i][j];
                if (-1.0 == sim) {
                    frameCorrection++;
                }
                if (maxI < sim) {
                    maxI = sim;
                }
            }
            maxLeftSide += maxI;
        }

        for (int j = 0; j < frameWidth; j++) {
            double maxJ = 0;
            for (int i = 0; i < frameHeight; i++) {
                double sim = similarities[i][j];
                if (-1.0 == sim) {
                    frameCorrection++;
                }
                if (maxJ < sim) {
                    maxJ = sim;
                }
            }
            maxRightSide += maxJ;

        }
        InstanceMatcher.logger.debug("{} {}", maxLeftSide, maxRightSide);
        return (maxLeftSide + maxRightSide) / (frameWidth + frameHeight - frameCorrection);
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
