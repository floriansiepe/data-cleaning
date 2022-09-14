package florian.siepe;

import florian.siepe.blocker.TransformerRestClient;
import florian.siepe.control.io.KnowledgeBase;
import florian.siepe.entity.kb.WebTables;
import florian.siepe.evaluation.Evaluation;
import florian.siepe.matcher.PropertyMatcher;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.slf4j.Logger;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

@CommandLine.Command(mixinStandardHelpOptions = true)
public class CleaningCommand implements Runnable {

    private static final Logger logger = getLogger(CleaningCommand.class);
    private final TransformerRestClient transformerRestClient;
    @CommandLine.Option(names = {"-kb", "--knowledge-base"}, description = "Knowledge base", paramLabel = "KB", required = true)
    List<File> knowledgeBase;
    @CommandLine.Option(names = {"-t", "--tables"}, description = "Tables to clean", paramLabel = "TABLES", required = true)
    List<File> tables;

    @CommandLine.Option(names = "-g, --gold-standard", defaultValue = "data/gs_property.csv")
    File goldStandard;

    @ConfigProperty(name = "property-matcher.use-word2vec", defaultValue = "false")
    Boolean useWord2Vec;

    public CleaningCommand(@RestClient final TransformerRestClient transformerRestClient) {
        this.transformerRestClient = transformerRestClient;
    }

    @Override
    public void run() {
        // Initialize
        CleaningCommand.logger.info("Start building Knowledgebase index");
        var kb = KnowledgeBase.getInstance(this.knowledgeBase);

        CleaningCommand.logger.info("Build Knowledgebase index");
        CleaningCommand.logger.info("Loading webtables");
        var web = WebTables.loadWebTables(this.tables, true, true, true);
        CleaningCommand.logger.info("Loaded webtables");
        CleaningCommand.logger.info("Property matching");

        // Adjust threshold
        final var threshold = 0.5;

        var propertyMatcher = new PropertyMatcher(this.useWord2Vec, threshold);
        var correspondencesMap = propertyMatcher.runMatching(kb.getKnowledgeIndex(), web);
        CleaningCommand.logger.info("Instance matching");

        // Uncomment to run instance matching
        /*final var instanceMatcher = new InstanceMatcher(kb.getKnowledgeIndex(), web, transformerRestClient);
        final var correspondencesMap = instanceMatcher.runMatching();*/

        var flattenedCorrespondences = correspondencesMap.values()
                .stream()
                .flatMap(correspondenceProcessable -> correspondenceProcessable.get().stream())
                .filter(correspondence -> correspondence.getSimilarityScore() >= threshold)
                .collect(Collectors.toList());

        try {
            var evaluation = new Evaluation(kb.getKnowledgeIndex(), web);
            evaluation.loadGoldStandard(this.goldStandard);
            evaluation.evaluate(flattenedCorrespondences);
            CleaningCommand.logger.warn("Threshold was: {}", threshold);
        } catch (final IOException e) {
            e.printStackTrace();
        }


    }

}
