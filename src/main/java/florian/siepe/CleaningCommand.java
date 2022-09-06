package florian.siepe;

import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.index.io.DefaultIndex;
import de.uni_mannheim.informatik.dws.winter.index.io.InMemoryIndex;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import florian.siepe.blocker.TransformerBlocker;
import florian.siepe.blocker.TransformerRestClient;
import florian.siepe.control.io.KnowledgeBase;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.MatchableTableRow;
import florian.siepe.entity.kb.WebTables;
import florian.siepe.evaluation.Evaluation;
import florian.siepe.matcher.InstanceMatcher;
import florian.siepe.matcher.PropertyMatcher;
import florian.siepe.t2k.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.slf4j.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static java.util.stream.Collectors.groupingBy;
import static org.slf4j.LoggerFactory.getLogger;

@Command(mixinStandardHelpOptions = true)
public class CleaningCommand implements Runnable {

    private static final Logger logger = getLogger(CleaningCommand.class);
    private final TransformerRestClient transformerRestClient;
    @Option(names = {"-o", "--ontology"}, description = "Ontology to use (Class hierarchy)", paramLabel = "ONTOLOGY", required = true)
    File ontology;
    @Option(names = {"-kb", "--knowledge-base"}, description = "Knowledge base", paramLabel = "KB", required = true)
    List<File> knowledgeBase;
    @Option(names = {"-t", "--tables"}, description = "Tables to clean", paramLabel = "TABLES", required = true)
    List<File> tables;

    @Option(names = {"-g, --gold-standard"}, defaultValue = "data/gs_class.csv")
    File goldStandard;

    @Option(names = {"-sf", "--surface-forms"}, defaultValue = "data/surfaceforms.txt")
    File surfaceForms;

    @Option(names = {"-il", "--index-location"}, defaultValue = "data/index/")
    String indexLocation;

    @ConfigProperty(name = "property-matcher.use-word2vec", defaultValue = "false")
    Boolean useWord2Vec;

    public CleaningCommand(@RestClient TransformerRestClient transformerRestClient) {
        this.transformerRestClient = transformerRestClient;
    }

    @Override
    public void run() {
        // Initialize
        final var sf = new SurfaceForms(surfaceForms, null);
        boolean createIndex = false;
        IIndex index;
        // create index for candidate lookup
        if(indexLocation==null) {
            // no index provided, create a new one in memory
            index = new InMemoryIndex();
            createIndex = true;
        } else{
            // load index from location that was provided
            index = new DefaultIndex(indexLocation);
            final var dir = new File(indexLocation);
            createIndex = !dir.exists() || dir.listFiles().length == 0;
        }
        if(createIndex) {
            sf.loadIfRequired();
        }

        logger.info("Start building Knowledgebase index");
        final var kb = KnowledgeBase.getInstance(knowledgeBase, createIndex ? index : null, sf);

        logger.info("Build Knowledgebase index");
        logger.info("Loading webtables");
        final var web = WebTables.loadWebTables(tables, true, true, true);
        logger.info("Loaded webtables");
        logger.info("Instance matching");

        MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine = new MatchingEngine<>();

        // create schema correspondences between the key columns and rdfs:Label
        /*Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> keyCorrespondences = web.getKeys().map(new WebTableKeyToRdfsLabelCorrespondenceGenerator(kb.getKnowledgeIndex().getRdfsLabel()));

        // Candidate selection
        CandidateSelection cs = new CandidateSelection(matchingEngine, index, indexLocation, web, kb, sf, keyCorrespondences);
        Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences = cs.run();

        // Candidate Decision
        ClassDecision classDec = new ClassDecision();
        Map<Integer, Set<String>> classesPerTable = classDec.runClassDecision(kb, instanceCorrespondences, matchingEngine);

        // Candidate refinement
        CandidateRefinement cr = new CandidateRefinement(matchingEngine,  index, indexLocation, web, kb, sf, keyCorrespondences, classesPerTable);
        instanceCorrespondences = cr.run();*/
        final var instanceMatcher = new InstanceMatcher(kb.getKnowledgeIndex(), web, transformerRestClient);
        final var correspondencesMap = instanceMatcher.runMatching();

        logger.info("Instance matching");
        var threshold = 0.5;
        /*final var propertyMatcher = new PropertyMatcher(useWord2Vec, threshold);
        final var correspondencesMap = propertyMatcher.runMatching(kb.getKnowledgeIndex(), web);*/

        final var classMapping = new HashMap<Integer, Set<Integer>>();
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

            final var matchingQuality = new HashMap<Integer, Double>();
            for (final var e : matching.entrySet()) {
                final var cor = e.getValue();
                final var ontologyTableColumn = cor.getFirstRecord();
                final var similarityScore = cor.getSimilarityScore();
                matchingQuality.putIfAbsent(ontologyTableColumn.getTableId(), similarityScore);
                matchingQuality.computeIfPresent(ontologyTableColumn.getTableId(), (key, sim) -> sim + similarityScore);
                logger.info(e.getKey().toString());
                logger.info("{} <-> {}: {}", cor.getFirstRecord().toString(), cor.getSecondRecord().toString(), cor.getSimilarityScore());
            }

            /*final var bestClass = matchingQuality.entrySet().stream().max(Map.Entry.comparingByValue()).orElse(null);
            if (bestClass != null) {
                classMapping.put(webTableId, bestClass.getKey());
            }*/

            classMapping.put(webTableId, matchingQuality.keySet());




            /*final var engine = new MatchingEngine<MatchableTableColumn, MatchableTableColumn>();
            final var maximumWeightGlobalSchemaMatching = engine.getMaximumWeightGlobalInstanceMatching(correspondences);
            for (final var cor : maximumWeightGlobalSchemaMatching.get()) {
                classes.add(cor.getSecondRecord().getTableId());
                logger.info("{} <-> {}: {}", cor.getFirstRecord().toString(), cor.getSecondRecord().toString(), cor.getSimilarityScore());
            }*/


        }

        try {
            logger.info("Class mapping {}", classMapping);
            final var evaluation = new Evaluation(kb.getKnowledgeIndex(), web);
            evaluation.loadGoldStandard(goldStandard);
            evaluation.evaluate(classMapping, threshold);
            logger.warn("Threshold was: {}", threshold);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
