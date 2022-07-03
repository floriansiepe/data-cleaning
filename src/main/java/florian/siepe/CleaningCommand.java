package florian.siepe;

import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import florian.siepe.control.graph.GraphFactory;
import florian.siepe.control.io.KnowledgeBase;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.WebTables;
import florian.siepe.matcher.PropertyMatcher;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

@Command(mixinStandardHelpOptions = true)
public class CleaningCommand implements Runnable {

    private static final Logger logger = getLogger(CleaningCommand.class);
    private final GraphFactory graphFactory;
    @Option(names = {"-o", "--ontology"}, description = "Ontology to use (Class hierarchy)", paramLabel = "ONTOLOGY", required = true)
    File ontology;
    @Option(names = {"-kb", "--knowledge-base"}, description = "Knowledge base", paramLabel = "KB", required = true)
    List<File> knowledgeBase;
    @Option(names = {"-t", "--tables"}, description = "Tables to clean", paramLabel = "TABLES", required = true)
    List<File> tables;

    @ConfigProperty(name = "property-matcher.use-word2vec", defaultValue = "false")
    Boolean useWord2Vec;

    public CleaningCommand(final GraphFactory graphFactory) {
        this.graphFactory = graphFactory;
    }

    @Override
    public void run() {
        logger.info("Start building Knowledgebase index");
        final var kb = KnowledgeBase.getInstance(knowledgeBase);
        logger.info("Build Knowledgebase index");
        logger.info("Loading webtables");
        final var web = WebTables.loadWebTables(tables, false, true, true);
        logger.info("Loaded webtables");

        logger.info("Property matching");
        final var propertyMatcher = new PropertyMatcher(useWord2Vec);
        final var correspondencesMap = propertyMatcher.runMatching(kb.getKnowledgeIndex(), web);

        for (final var entry : correspondencesMap.entrySet()) {
            final var classes = new HashSet<Integer>();
            final var matching = new HashMap<MatchableTableColumn, Correspondence<MatchableTableColumn, MatchableTableColumn>>();
            final var webTableId = entry.getKey();
            final var correspondences = entry.getValue();
            logger.info("Correspondences for {}", web.getTableNames().get(webTableId));

            for (final var cor : correspondences.get()) {
                if (matching.containsKey(cor.getSecondRecord())) {
                    if (matching.get(cor.getSecondRecord()).getSimilarityScore() < cor.getSimilarityScore()) {
                        matching.put(cor.getSecondRecord(), cor);
                    }
                } else {
                    matching.put(cor.getSecondRecord(), cor);
                }
                classes.add(cor.getFirstRecord().getTableId());
            }

            for (final var e : matching.entrySet()) {
                var cor = e.getValue();
                logger.info(e.getKey().toString());
                logger.info("{} <-> {}: {}", cor.getFirstRecord().toString(), cor.getSecondRecord().toString(), cor.getSimilarityScore());
            }


            logger.info("Maximal matching");



            /*final var engine = new MatchingEngine<MatchableTableColumn, MatchableTableColumn>();
            final var maximumWeightGlobalSchemaMatching = engine.getMaximumWeightGlobalInstanceMatching(correspondences);
            for (final var cor : maximumWeightGlobalSchemaMatching.get()) {
                classes.add(cor.getSecondRecord().getTableId());
                logger.info("{} <-> {}: {}", cor.getFirstRecord().toString(), cor.getSecondRecord().toString(), cor.getSimilarityScore());
            }*/


        }


        /*logger.info("Instance matching");

        final var instanceMatcher = new InstanceMatcher(correspondencesMap, kb.getKnowledgeIndex(), web);
        instanceMatcher.runMatching();*/


    }

}
