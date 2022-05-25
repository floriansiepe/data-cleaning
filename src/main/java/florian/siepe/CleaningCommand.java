package florian.siepe;

import florian.siepe.control.graph.GraphFactory;
import florian.siepe.control.io.KnowledgeBase;
import florian.siepe.entity.kb.WebTables;
import florian.siepe.matcher.PropertyBlocker;
import org.slf4j.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
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

        final var propertyBlocker = new PropertyBlocker();
        final var correspondencesMap = propertyBlocker.runBlocking(kb.getKnowledgeIndex(), web);

        for (final var entry : correspondencesMap.entrySet()) {
            final var tableId = entry.getKey();
            final var correspondences = entry.getValue();
            logger.info("Correspondences for {}", tableId);
            for (final var cor : correspondences.get()) {
                logger.info("{} <-> {}: {}", cor.getFirstRecord().getHeader(), cor.getSecondRecord().getHeader(), cor.getSimilarityScore());
            }

        }



        /*
        final var graph = graphFactory.buildClassHierarchy(ontology);
        final var dot = GraphUtils.writeDot(graph);

        final var file = new File("graph.dot");

        try (var w = new FileWriter(file)) {
           w.write(dot);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }*/
    }

}
