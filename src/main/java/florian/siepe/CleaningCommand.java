package florian.siepe;

import florian.siepe.blocker.TransformerRestClient;
import florian.siepe.control.io.KnowledgeBase;
import florian.siepe.entity.kb.WebTables;
import florian.siepe.evaluation.Evaluation;
import florian.siepe.matcher.InstanceMatcher;
import florian.siepe.matcher.MatcherType;
import florian.siepe.matcher.PropertyMatcher;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Command(mixinStandardHelpOptions = true)
public class CleaningCommand implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CleaningCommand.class);
    private final TransformerRestClient transformerRestClient;
    @Parameters(index = "0", description = "Matcher to use: property or instance")
    MatcherType matcherType;
    @Option(names = {"-kb", "--knowledge-base"}, description = "Knowledge base", paramLabel = "KB", required = true)
    List<File> knowledgeBase;
    @Option(names = {"-t", "--tables"}, description = "Tables to clean", paramLabel = "TABLES", required = true)
    List<File> tables;

    @Option(names = {"-g", "--gold-standard"}, defaultValue = "data/gs_property.csv")
    File goldStandard;
    @Option(names = {"-t", "--threshold"}, required = true)
    double threshold;

    @Option(names = {"-w", "--word2vec"}, defaultValue = "false")
    boolean useWord2Vec;

    public CleaningCommand(@RestClient final TransformerRestClient transformerRestClient) {
        this.transformerRestClient = transformerRestClient;
    }

    @Override
    public void run() {
        logger.info("Start building Knowledgebase index");
        var kb = KnowledgeBase.getInstance(this.knowledgeBase);

        logger.info("Build Knowledgebase index");
        logger.info("Loading webtables");
        var web = WebTables.loadWebTables(this.tables, true, true, true);
        logger.info("Loaded webtables");
        logger.info("{} matching", matcherType.toString());

        var matcher = matcherType.equals(MatcherType.instance) ? new InstanceMatcher(kb.getKnowledgeIndex(), web, transformerRestClient) : new PropertyMatcher(kb.getKnowledgeIndex(), web, this.useWord2Vec, threshold);

        var correspondencesMap = matcher.runMatching();

        var flattenedCorrespondences = correspondencesMap.values()
                .stream()
                .flatMap(correspondenceProcessable -> correspondenceProcessable.get().stream())
                .filter(correspondence -> correspondence.getSimilarityScore() >= threshold)
                .collect(Collectors.toList());

        try {
            var evaluation = new Evaluation(kb.getKnowledgeIndex(), web);
            evaluation.loadGoldStandard(this.goldStandard);
            evaluation.evaluate(flattenedCorrespondences);
            logger.warn("Threshold was: {}", threshold);
        } catch (final IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

}
