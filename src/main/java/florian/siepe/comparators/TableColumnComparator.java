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

    public TableColumnComparator(final SimilarityMeasure<String> similarity) {
        this.similarity = similarity;
    }

    @Override
    public double compare(final MatchableTableColumn record1, final MatchableTableColumn record2, final Correspondence<MatchableTableColumn, Matchable> correspondence) {
        double sim = this.similarity.calculate(record1.getHeader(), record2.getHeader());
        if (this.comparisonLog != null) {
            this.comparisonLog.setComparatorName(this.getClass().getName());
            this.comparisonLog.setRecord1Value(record1.getHeader());
            this.comparisonLog.setRecord2Value(record2.getHeader());
            this.comparisonLog.setSimilarity(Double.toString(sim));
        }

        return sim;
    }


    @Override
    public void setComparisonLog(final ComparatorLogger comparisonLog) {
        this.comparisonLog = comparisonLog;
    }
}
