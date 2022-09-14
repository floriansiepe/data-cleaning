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
import java.util.*;

public class Evaluation {
    private static final Logger logger = LoggerFactory.getLogger(Evaluation.class);

    private MatchingGoldStandard goldStandard;
    private final KnowledgeIndex index;
    private final WebTables webTables;

    public Evaluation(KnowledgeIndex index, WebTables webTables) {
        this.index = index;
        this.webTables = webTables;
    }

    public void loadGoldStandard(final File goldStandard) throws IOException {
        this.goldStandard = new MatchingGoldStandard();
        this.goldStandard.loadFromCSVFile(goldStandard);
        this.goldStandard.setComplete(true);
    }

    public void evaluate(HashMap<Integer, Set<Integer>> classMapping, double threshold) {
        var matchCount = 0;
        Evaluation.logger.info("Found the following classes");
        for (var matching : classMapping.entrySet()) {
            for (Integer ontologyTableId : matching.getValue()) {
                var ontologyTable = this.index.getTableIds().inverse().get(ontologyTableId);
                var webTable = this.webTables.getTablesById().get(matching.getKey());
                final var matched = this.goldStandard.containsPositive(webTable.getPath(), ontologyTable);
                Evaluation.logger.info("{} - {} / {}", ontologyTable, webTable.getPath(), matched);
                if (matched) {
                    matchCount++;
                    break;
                }
            }
        }
        Evaluation.logger.warn("Matched {} of {}", matchCount, classMapping.size());
        this.evaluateClassCorrespondences(this.createClassCorrespondence(classMapping), "label-based-" + threshold);
    }

    protected Processable<Correspondence<MatchableTable, MatchableTableColumn>> createClassCorrespondence(final Map<Integer, Set<Integer>> classPerTable) {
        //TODO the class matching should be replaced by actual matchers that create correspondences, such that we don't need this method
        final Processable<Correspondence<MatchableTable, MatchableTableColumn>> result = new ProcessableCollection<>();

        for (final int tableId : classPerTable.keySet()) {

            final MatchableTable webTable = this.webTables.getTables().getRecord(this.webTables.getTableNames().get(tableId));

            for (Integer indexTableId : classPerTable.get(tableId)) {
                final String className = this.index.getClassIndex(indexTableId);

                final MatchableTable kbTable = this.index.getTables().getRecord(className);

                final Correspondence<MatchableTable, MatchableTableColumn> cor = new Correspondence<MatchableTable, MatchableTableColumn>(webTable, kbTable, 1.0, null);
                result.add(cor);
            }

        }

        return result;
    }

    protected void evaluateClassCorrespondences(final Processable<Correspondence<MatchableTable, MatchableTableColumn>> classCorrespondences, final String name) {
        Performance performance = null;
        if (null != goldStandard) {
            classCorrespondences.distinct();
            final MatchingEvaluator<MatchableTable, MatchableTableColumn> classEvaluator = new MatchingEvaluator<>();
            final Collection<Correspondence<MatchableTable, MatchableTableColumn>> classCorrespondencesCollection = classCorrespondences.get();
            System.out.printf("%d %s class correspondences%n", classCorrespondencesCollection.size(), name);
            performance = classEvaluator.evaluateMatching(classCorrespondencesCollection, this.goldStandard);
        }

        if (null != performance) {
            System.out
                    .printf(
                            "Class Performance:\n\tPrecision: %.4f\n\tRecall: %.4f\n\tF1: %.4f%n",
                            performance.getPrecision(), performance.getRecall(),
                            performance.getF1());
        }
    }

    public void evaluate(List<Correspondence<MatchableTableColumn, MatchableTableColumn>> correspondences) {
        var evaluator = new MatchingEvaluator<MatchableTableColumn, MatchableTableColumn>();
        var performance = evaluator.evaluateMatching(correspondences, this.goldStandard);
        System.out.printf("Column Performance:\n\tPrecision: %.4f\n\tRecall: %.4f\n\tF1: %.4f%n", performance.getPrecision(), performance.getRecall(), performance.getF1());
    }
}
