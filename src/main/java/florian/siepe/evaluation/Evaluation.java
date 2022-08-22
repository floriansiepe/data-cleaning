package florian.siepe.evaluation;

import de.uni_mannheim.informatik.dws.winter.matching.MatchingEvaluator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import florian.siepe.control.io.KnowledgeIndex;
import florian.siepe.entity.kb.MatchableTable;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.WebTables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Evaluation {
    private static final Logger logger = LoggerFactory.getLogger(Evaluation.class);

    private MatchingGoldStandard classGs;
    private final KnowledgeIndex index;
    private final WebTables webTables;

    public Evaluation(final KnowledgeIndex index, final WebTables webTables) {
        this.index = index;
        this.webTables = webTables;
    }

    public void loadGoldStandard(File goldStandard) throws IOException {
        classGs = new MatchingGoldStandard();
        classGs.loadFromCSVFile(goldStandard);
        classGs.setComplete(true);
    }

    public void evaluate(final HashMap<Integer, Set<Integer>> classMapping, final double threshold) {
       var matchCount = 0;
       logger.info("Found the following classes");
        for (final var matching : classMapping.entrySet()) {
            for (final Integer ontologyTableId : matching.getValue()) {
                final var ontologyTable = index.getTableIds().inverse().get(ontologyTableId);
                final var webTable = webTables.getTablesById().get(matching.getKey());
                var matched = classGs.containsPositive(webTable.getPath(), ontologyTable);
                logger.info("{} - {} / {}", ontologyTable, webTable.getPath(), matched);
                if (matched) {
                    matchCount++;
                    break;
                }
            }
        }
        logger.warn("Matched {} of {}", matchCount, classMapping.size());
        evaluateClassCorrespondences(createClassCorrespondence(classMapping), "label-based-" + threshold);
    }

    protected Processable<Correspondence<MatchableTable, MatchableTableColumn>> createClassCorrespondence(Map<Integer, Set<Integer>> classPerTable) {
        //TODO the class matching should be replaced by actual matchers that create correspondences, such that we don't need this method
        Processable<Correspondence<MatchableTable, MatchableTableColumn>> result = new ProcessableCollection<>();

        for(int tableId : classPerTable.keySet()) {

            MatchableTable webTable = webTables.getTables().getRecord(webTables.getTableNames().get(tableId));

            for (final Integer indexTableId : classPerTable.get(tableId)) {
                String className =  index.getClassIndex(indexTableId);

                MatchableTable kbTable = index.getTables().getRecord(className);

                Correspondence<MatchableTable, MatchableTableColumn> cor = new Correspondence<MatchableTable, MatchableTableColumn>(webTable, kbTable, 1.0, null);
                result.add(cor);
            }

        }

        return result;
    }

    protected void evaluateClassCorrespondences(Processable<Correspondence<MatchableTable, MatchableTableColumn>> classCorrespondences, String name) {
        Performance classPerf = null;
        if(classGs!=null) {
            classCorrespondences.distinct();
            MatchingEvaluator<MatchableTable, MatchableTableColumn> classEvaluator = new MatchingEvaluator<>();
            Collection<Correspondence<MatchableTable, MatchableTableColumn>> classCorrespondencesCollection = classCorrespondences.get();
            System.out.println(String.format("%d %s class correspondences", classCorrespondencesCollection.size(), name));
            classPerf = classEvaluator.evaluateMatching(classCorrespondencesCollection, classGs);
        }

        if(classPerf!=null) {
            System.out
                    .println(String.format(
                            "Class Performance:\n\tPrecision: %.4f\n\tRecall: %.4f\n\tF1: %.4f",
                            classPerf.getPrecision(), classPerf.getRecall(),
                            classPerf.getF1()));
        }
    }
}
