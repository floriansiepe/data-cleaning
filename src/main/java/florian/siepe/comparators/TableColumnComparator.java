package florian.siepe.comparators;

import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.Comparator;
import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.ComparatorLogger;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import florian.siepe.entity.kb.MatchableTableColumn;

public class TableColumnComparator implements Comparator<MatchableTableColumn, MatchableTableColumn> {
    private final SimilarityMeasure<String> similarity;
    private ComparatorLogger comparisonLog;

    public TableColumnComparator(SimilarityMeasure<String> similarity) {
        this.similarity = similarity;
    }

    @Override
    public double compare(MatchableTableColumn record1, MatchableTableColumn record2, Correspondence<MatchableTableColumn, Matchable> correspondence) {
        final double sim = similarity.calculate(record1.getHeader(), record2.getHeader());
        if (null != this.comparisonLog) {
            comparisonLog.setComparatorName(getClass().getName());
            comparisonLog.setRecord1Value(record1.getHeader());
            comparisonLog.setRecord2Value(record2.getHeader());
            comparisonLog.setSimilarity(Double.toString(sim));
        }

        return sim;
    }


    @Override
    public void setComparisonLog(ComparatorLogger comparisonLog) {
        this.comparisonLog = comparisonLog;
    }
}
