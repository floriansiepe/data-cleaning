package florian.siepe.matcher;

import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.similarity.string.JaccardOnNGramsSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;
import florian.siepe.blocker.LocalitySensitiveHashingBlockingKeyGenerator;
import florian.siepe.blocker.TransformerBlocker;
import florian.siepe.blocker.TransformerRestClient;
import florian.siepe.control.SampleExtractor;
import florian.siepe.control.ValueExtractor;
import florian.siepe.control.io.KnowledgeIndex;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.MatchableTableRow;
import florian.siepe.entity.kb.WebTables;
import florian.siepe.measures.MaxStringSimilarityMeasure;
import florian.siepe.measures.RowSimilarity;
import florian.siepe.measures.WebStringNormalizingSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final ValueExtractor valueExtractor;
    private final TransformerBlocker transformerBlocker;
    private final SampleExtractor sampleExtractor;

    public InstanceMatcher(KnowledgeIndex index, WebTables webTables, TransformerRestClient transformerRestClient) {
        this.index = index;
        this.webTables = webTables;
        this.valueExtractor = new ValueExtractor(index, webTables);
        this.transformerBlocker = new TransformerBlocker(transformerRestClient, index, webTables);
        sampleExtractor = new SampleExtractor();
    }

    public Map<Integer, Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>>> runMatching() {
        final var correspondences = new HashMap<Integer, Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>>>();
        final var webTableColumnsByTableId = webTables.getSchema().get().stream().collect(groupingBy(MatchableTableColumn::getTableId));

        for (final var entry : webTableColumnsByTableId.entrySet()) {
            final var tableCorrespondences = new ProcessableCollection<Correspondence<MatchableTableColumn, MatchableTableColumn>>();
            final var webTableId = entry.getKey();
            final var webTableColumns = entry.getValue();
            final var blockedCandidates = transformerBlocker.generateCandidates(webTableColumns);
            for (final var blockedPair : blockedCandidates.entrySet()) {
                final var webColumn = blockedPair.getKey();
                final var blockedIndexColumns = blockedPair.getValue();

                final var correspondence = runMatching(blockedIndexColumns, webColumn);
                if (correspondence != null) {
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


    private Correspondence<MatchableTableColumn, MatchableTableColumn> runMatching(final Collection<MatchableTableColumn> indexColumns, final MatchableTableColumn webTableColumn) {
        logger.info("Run matching for {} with {} blocking candidates", webTableColumn.toString(), indexColumns.size());
        Correspondence<MatchableTableColumn, MatchableTableColumn> bestCorrespondence = null;

        for (final MatchableTableColumn indexColumn : indexColumns) {
            // Do a quick null check since this can happen
            if (indexColumn != null && webTableColumn != null && indexColumn.getType().equals(webTableColumn.getType())) {
                final var matching = runMatching(indexColumn, webTableColumn);
                if (bestCorrespondence == null) {
                    bestCorrespondence = matching;
                }
                if (bestCorrespondence.getSimilarityScore() < matching.getSimilarityScore()) {
                    bestCorrespondence = matching;
                }
            }
        }
        return bestCorrespondence;
    }

    private Correspondence<MatchableTableColumn, MatchableTableColumn> runMatching(MatchableTableColumn indexColumn, MatchableTableColumn webColumn) {
        logger.debug("Start Matching");
        if (!indexColumn.getType().equals(webColumn.getType())) {
            throw new RuntimeException("Data types are not matching: " + indexColumn.getType() + " and " + webColumn.getType());
        }

         List<MatchableTableRow> firstRecords = new ArrayList<>(valueExtractor.findIndexRecords(indexColumn));
         List<MatchableTableRow> secondRecords =new ArrayList<>(valueExtractor.findWebRecords(webColumn));

        if (firstRecords.size() > Math.pow(secondRecords.size(), 2) || firstRecords.size() > 5000) {
            // Record size is totally unbalanced. Hashing might take too long
            final int maxRecords = Math.min((int) Math.pow(secondRecords.size(), 2), 5000);
            firstRecords = sampleExtractor.extractValue(maxRecords, firstRecords, t -> t);
        }

        logger.debug("Finished values extraction");
        final var firstDataSet = new HashedDataSet<MatchableTableRow, MatchableTableColumn>(firstRecords);
        firstDataSet.addAttribute(indexColumn);
        final var secondDataSet = new HashedDataSet<MatchableTableRow, MatchableTableColumn>(secondRecords);
        secondDataSet.addAttribute(webColumn);

        logger.debug("Instance matching dataset sizes: {}, {}", firstDataSet.size(), secondDataSet.size());
        final var firstColumnIndex = indexColumn.getColumnIndex();
        final var secondColumnIndex = webColumn.getColumnIndex();

        logger.debug("Start hashing");
        final var localitySensitiveHashingBlockingKeyGenerator = new LocalitySensitiveHashingBlockingKeyGenerator(webTables, index, new HashSet<>(firstRecords), new HashSet<>(secondRecords), firstColumnIndex, secondColumnIndex);

        final var bucketsFirst = generateBlocks(firstRecords, localitySensitiveHashingBlockingKeyGenerator);
        final var bucketsSecond = generateBlocks(secondRecords, localitySensitiveHashingBlockingKeyGenerator);
        bucketsFirst.remove(-1);
        bucketsSecond.remove(-1);
        logger.debug("Finished hashing");

        /*var tfidfSim = new TFIDFCosineSimilarity<MatchableTableRow, MatchableTableColumn, MatchableTableColumn>(firstDataSet, secondDataSet, new TokenGenerator<MatchableTableRow, MatchableTableColumn>() {
            Map<MatchableTableRow, String[]> tokenMap = new HashMap<>();
            public void TFIDFCosineSimilarity() {
                preComputeTokens(firstDataSet, firstColumnIndex);
                preComputeTokens(secondDataSet, secondColumnIndex);
            }

            private void preComputeTokens(DataSet<MatchableTableRow, MatchableTableColumn> dataSet, int index) {
                dataSet.foreach(row -> tokenMap.put(row, tokenizeString(String.valueOf(row.get(index)))));
            }

            @Override
            public void generateTokens(final MatchableTableRow record, final Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences, final DataIterator<Pair<String, MatchableTableRow>> resultCollector) {
                if (!tokenMap.containsKey(record)) {
                    return;
                }
                String[] tokens = tokenMap.get(record);

                for (String token : tokens) {
                    resultCollector.next(new Pair<>(token, record));
                }
            }

            @Override
            public String[] tokenizeString(final String value) {
                return value.split(" ");
            }
        });*/

        //final var similarityMeasure = new WebStringNormalizingSimilarity(new MaxStringSimilarityMeasure(new LevenshteinSimilarity(), new JaccardOnNGramsSimilarity(2)));
        final var similarityMeasure = new RowSimilarity(firstColumnIndex, secondColumnIndex, indexColumn.getType());
        double blockSimilaritySum = 0;
        int blockCount = 0;

        for (final Map.Entry<Integer, List<MatchableTableRow>> bucketFirst : bucketsFirst.entrySet()) {
            if (bucketFirst.getValue().isEmpty()) {
                continue;
            }
            final var bucketSecond = bucketsSecond.get(bucketFirst.getKey());

            if (bucketSecond == null || bucketSecond.isEmpty()) {
                continue;
            }

            var similarities = new double[bucketSecond.size()][bucketFirst.getValue().size()];

            for (int i = 0; i < bucketSecond.size(); i++) {
                var rowSecond = bucketSecond.get(i);

                for (int j = 0; j < bucketFirst.getValue().size(); j++) {
                   var rowFirst = bucketFirst.getValue().get(j);
                    try {
                        /*final String first = rowFirst.get(firstColumnIndex).toString();
                        final String second = rowSecond.get(secondColumnIndex).toString();*/
                        final var sim = similarityMeasure.calculate(rowFirst, rowSecond);
                        similarities[i][j] = sim;
                    } catch (NullPointerException e) {
                        logger.debug("Got a null value while trying to fetch value: {}", e.getMessage());
                    }
                }
            }

            final double blockSimilarity = computeBlockSimilarity(similarities);
            blockSimilaritySum += blockSimilarity;
            blockCount++;
        }
        double averageSimilarity = blockSimilaritySum / blockCount;

        if (averageSimilarity > 0.5d) {
            logger.warn("Match: {} {} <-> {}", averageSimilarity, indexColumn, webColumn);
        }
        return new Correspondence<>(indexColumn, webColumn, averageSimilarity);
    }

    private static Map<Integer, List<MatchableTableRow>> generateBlocks(final List<MatchableTableRow> records, final LocalitySensitiveHashingBlockingKeyGenerator localitySensitiveHashingBlockingKeyGenerator) {
        return  records
                .stream()
                .flatMap(record -> localitySensitiveHashingBlockingKeyGenerator.getKeys(record).map(blockId -> new AbstractMap.SimpleEntry<>(blockId, record)))
                .collect(groupingBy(AbstractMap.SimpleEntry::getKey))
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().stream().map(AbstractMap.SimpleEntry::getValue).collect(Collectors.toList())));
    }

    private double computeBlockSimilarity(final double[][] similarities) {
        // Formula is from https://dbs.uni-leipzig.de/file/BTW-Workshop_2007_EngmannMassmann.pdf 2.3 Content-based Matching
        int frameHeight = similarities.length;
        if (frameHeight == 0) {
            return 0;
        }
        int frameWidth = similarities[0].length;

        double maxLeftSide = 0;
        double maxRightSide = 0;

        for (int i = 0; i < frameHeight; i++) {
            double maxI = 0;
            for (int j = 0; j < frameWidth; j++) {
               if (maxI < similarities[i][j]) {
                   maxI = similarities[i][j];
               }
            }
            maxLeftSide+=maxI;
        }

        for (int j = 0; j < frameWidth; j++) {
            double maxJ = 0;
            for (int i = 0; i < frameHeight; i++) {
                if (maxJ < similarities[i][j]) {
                    maxJ = similarities[i][j];
                }
            }
            maxRightSide += maxJ;

        }
        logger.debug("{} {}", maxLeftSide, maxRightSide);
        return (maxLeftSide + maxRightSide) / (frameWidth + frameHeight);
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
