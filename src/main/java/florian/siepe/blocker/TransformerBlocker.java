package florian.siepe.blocker;

import de.uni_mannheim.informatik.dws.winter.webtables.WebTablesStringNormalizer;
import florian.siepe.control.SampleExtractor;
import florian.siepe.control.ValueExtractor;
import florian.siepe.control.io.KnowledgeIndex;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.WebTables;
import florian.siepe.entity.transformer.BlockingCandidateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;


public class TransformerBlocker {
    private static final Logger logger = LoggerFactory.getLogger(TransformerBlocker.class);
    private static int K = 100;
    private final ValueExtractor valueExtractor;
    private final SampleExtractor sampleExtractor;
    private TransformerRestClient transformerClient;
    private KnowledgeIndex index;
    private WebTables webTables;

    public TransformerBlocker(final TransformerRestClient transformerClient, final KnowledgeIndex index, final WebTables webTables) {
        this.transformerClient = transformerClient;
        this.index = index;
        this.webTables = webTables;
        this.valueExtractor = new ValueExtractor(index, webTables);
        this.sampleExtractor = new SampleExtractor();
    }

    public Map<MatchableTableColumn, List<MatchableTableColumn>> generateCandidates(List<MatchableTableColumn> webTableColumns) {
        final HashMap<MatchableTableColumn, List<MatchableTableColumn>> candidates = new HashMap<>();
        for (final MatchableTableColumn webTableColumn : webTableColumns) {
            final var predictedClasses = transformerClient.predict(extractWebTableSamples(webTableColumn, K), 5);
            final var blockedCandidates = predictedClasses
                    .stream()
                    .collect(groupingBy(blockingCandidateResponse -> blockingCandidateResponse.key))
                    .values()
                    .stream()
                    .filter(blockingCandidateResponses -> !blockingCandidateResponses.isEmpty())
                    .map(blockingCandidateResponses -> {
                        var averageSim = blockingCandidateResponses
                                .stream()
                                .map(blockingCandidateResponse -> blockingCandidateResponse.prob)
                                .reduce(Double::sum)
                                .map(sum -> sum / blockingCandidateResponses.size())
                                .orElse(0d);
                        var aggregatedResponse = new BlockingCandidateResponse();
                        aggregatedResponse.prob = averageSim;
                        aggregatedResponse.key = blockingCandidateResponses.get(0).key;
                        return aggregatedResponse;
                    })
                    .sorted(Comparator.comparing(BlockingCandidateResponse::getProb, Comparator.reverseOrder()))
                    // This is just for now to filter http://www.w3.org/2000/01/rdf-schema#label
                    .filter(blockingCandidateResponse -> !blockingCandidateResponse.getKey().equals(index.getRdfsLabel().getIdentifier()))
                    .flatMap(blockingCandidateResponse -> {
                        var propertyId = index.getPropertyIds().get(blockingCandidateResponse.key);
                        if (propertyId == null) {
                            logger.info("Property {} is not indexed and cant be used to get a table mapping", blockingCandidateResponse.key);
                            return null;
                        }
                        return index.getPropertyClassIndices()
                                .get(propertyId)
                                .entrySet()
                                .stream()
                                .map(translation -> {
                                    final var tableId = translation.getKey();
                                    final var columnIndex = translation.getValue();
                                    return index.findColumn(tableId, columnIndex);
                                });
                    })
                    .filter(Objects::nonNull)
                    //.limit(5)
                    .collect(Collectors.toList());
            candidates.put(webTableColumn, blockedCandidates);
        }
        return candidates;
    }

    private List<String> extractWebTableSamples(MatchableTableColumn column, int k) {
        return sampleExtractor.extractValue(column, k, new ArrayList<>(valueExtractor.findWebRecords(column)))
                .stream()
                .map(v -> WebTablesStringNormalizer.normaliseValue(v, true))
                .collect(Collectors.toList());
    }
}
