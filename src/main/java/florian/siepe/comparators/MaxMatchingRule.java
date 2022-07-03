package florian.siepe.comparators;

import de.uni_mannheim.informatik.dws.winter.matching.rules.FilteringMatchingRule;
import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.Comparator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MaxMatchingRule<RecordType extends Matchable, SchemaElementType extends Matchable> extends FilteringMatchingRule<RecordType, SchemaElementType> {
    private final List<Comparator<RecordType, SchemaElementType>> comparators = new ArrayList<>();

    @SafeVarargs
    public MaxMatchingRule(final double finalThreshold, Comparator<RecordType, SchemaElementType>... comparators) {
        super(finalThreshold);
        this.comparators.addAll(Arrays.asList(comparators));
    }

    @Override
    public Correspondence<RecordType, SchemaElementType> apply(final RecordType record1, final RecordType record2, final Processable<Correspondence<SchemaElementType, Matchable>> schemaCorrespondences) {
        // double similarity = compare(record1, record2, null);
        double max = 0.0;
        Record debug = null;
        if (this.isDebugReportActive() && this.continueCollectDebugResults()) {
            debug = initializeDebugRecord(record1, record2, -1);
        }

        for (int i = 0; i < comparators.size(); i++) {

            Comparator<RecordType, SchemaElementType> comp = comparators.get(i);

            Correspondence<SchemaElementType, Matchable> correspondence = getCorrespondenceForComparator(
                    schemaCorrespondences, record1, record2, comp);

            if (this.isDebugReportActive()) {
                comp.getComparisonLog().initialise();
            }

            double similarity = comp.compare(record1, record2, correspondence);
            if (similarity > max) {
                max = similarity;
            }

            if (this.isDebugReportActive() && this.continueCollectDebugResults()) {
                debug = fillDebugRecord(debug, comp, i);
                addDebugRecordShort(record1, record2, comp, i);
            }
        }

        return new Correspondence<>(record1, record2, max, schemaCorrespondences);
    }

    @Override
    public double compare(final RecordType record1, final RecordType record2, final Correspondence<SchemaElementType, Matchable> correspondence) {
        double max = 0d;
        for (final Comparator<RecordType, SchemaElementType> comparator : comparators) {
            var sim = comparator.compare(record1, record2, correspondence);
            if (sim > max) {
                max = sim;
            }
        }
        return max;
    }

    public void addComparator(Comparator<RecordType, SchemaElementType> comparator) {
        this.comparators.add(comparator);
    }
}
