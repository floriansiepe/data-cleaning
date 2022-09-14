package florian.siepe.blocker;

import de.uni_mannheim.informatik.dws.winter.webtables.WebTablesStringNormalizer;
import florian.siepe.control.SampleExtractor;
import florian.siepe.control.ValueExtractor;
import florian.siepe.control.io.KnowledgeIndex;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.WebTables;
import florian.siepe.entity.transformer.BlockingCandidateResponse;

import java.util.*;
import java.util.stream.Collectors;


public class TransformerBlocker {
    private static final int K = 100;
    private final ValueExtractor valueExtractor;
    private final SampleExtractor sampleExtractor;
    private final TransformerRestClient transformerClient;
    private final KnowledgeIndex index;

    public TransformerBlocker(TransformerRestClient transformerClient, KnowledgeIndex index, WebTables webTables) {
        this.transformerClient = transformerClient;
        this.index = index;
        valueExtractor = new ValueExtractor(index, webTables);
        sampleExtractor = new SampleExtractor();
    }

    public Map<MatchableTableColumn, List<MatchableTableColumn>> generateCandidates(final List<MatchableTableColumn> webTableColumns) {
        HashMap<MatchableTableColumn, List<MatchableTableColumn>> candidates = new HashMap<>();
        for (MatchableTableColumn webTableColumn : webTableColumns) {
            var predictedColumns = this.transformerClient.predict(this.extractWebTableSamples(webTableColumn, TransformerBlocker.K), 5);
            var blockedCandidates = predictedColumns
                    .stream()
                    .sorted(Comparator.comparing(BlockingCandidateResponse::getProb, Comparator.reverseOrder()))
                    // This is just for now to filter http://www.w3.org/2000/01/rdf-schema#label
                    .filter(blockingCandidateResponse -> !blockingCandidateResponse.getKey().equals(this.index.getRdfsLabel().getIdentifier()))
                    .limit(10)
                    .flatMap(blockingCandidateResponse -> this.index.findColumn(blockingCandidateResponse.key).stream())
                    .filter(Objects::nonNull)
                    //.limit(5)
                    .collect(Collectors.toList());
            candidates.put(webTableColumn, blockedCandidates);
        }
        return candidates;
    }

    private List<String> extractWebTableSamples(final MatchableTableColumn column, final int k) {
        return this.sampleExtractor.extractValue(column, k, new ArrayList<>(this.valueExtractor.findWebRecords(column)))
                .stream()
                .map(v -> WebTablesStringNormalizer.normaliseValue(v, true))
                .collect(Collectors.toList());
    }
}
