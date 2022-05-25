package florian.siepe.comparators;

import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.Comparator;
import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.ComparatorLogger;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.measures.Word2VecSimilarity;

public class Word2VecComparator implements Comparator<MatchableTableColumn, MatchableTableColumn> {
    private final Word2VecSimilarity similarity;
    private ComparatorLogger comparisonLog;

    public Word2VecComparator(final Word2VecSimilarity similarity) {
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
