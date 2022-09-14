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
    public MaxMatchingRule(double finalThreshold, final Comparator<RecordType, SchemaElementType>... comparators) {
        super(finalThreshold);
        this.comparators.addAll(Arrays.asList(comparators));
    }

    @Override
    public Correspondence<RecordType, SchemaElementType> apply(RecordType record1, RecordType record2, Processable<Correspondence<SchemaElementType, Matchable>> schemaCorrespondences) {
        double max = 0.0;
        Record debug = null;
        if (isDebugReportActive() && continueCollectDebugResults()) {
            debug = this.initializeDebugRecord(record1, record2, -1);
        }

        for (int i = 0; i < this.comparators.size(); i++) {

            final Comparator<RecordType, SchemaElementType> comp = this.comparators.get(i);

            final Correspondence<SchemaElementType, Matchable> correspondence = this.getCorrespondenceForComparator(
                    schemaCorrespondences, record1, record2, comp);

            if (isDebugReportActive()) {
                comp.getComparisonLog().initialise();
            }

            final double similarity = comp.compare(record1, record2, correspondence);
            if (similarity > max) {
                max = similarity;
            }

            if (isDebugReportActive() && continueCollectDebugResults()) {
                debug = this.fillDebugRecord(debug, comp, i);
                this.addDebugRecordShort(record1, record2, comp, i);
            }
        }

        return new Correspondence<>(record1, record2, max, schemaCorrespondences);
    }

    @Override
    public double compare(RecordType record1, RecordType record2, Correspondence<SchemaElementType, Matchable> correspondence) {
        double max = 0.0d;
        for (Comparator<RecordType, SchemaElementType> comparator : this.comparators) {
            final var sim = comparator.compare(record1, record2, correspondence);
            if (sim > max) {
                max = sim;
            }
        }
        return max;
    }

    public void addComparator(final Comparator<RecordType, SchemaElementType> comparator) {
        comparators.add(comparator);
    }
}
