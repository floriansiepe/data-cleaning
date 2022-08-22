package florian.siepe.blocker;

import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.MatchableValue;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import florian.siepe.control.io.KnowledgeIndex;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.MatchableTableRow;
import florian.siepe.entity.kb.WebTables;
import info.debatty.java.lsh.LSHMinHash;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.Set;

import static org.slf4j.LoggerFactory.getLogger;

public class LocalitySensitiveHashingBlockingKeyGenerator extends BlockingKeyGenerator<MatchableTableRow, MatchableValue, MatchableTableRow> {
    private static final Logger logger = getLogger(LocalitySensitiveHashingBlockingKeyGenerator.class);
    private final MatchableTableRow[] universe;
    private final LSHMinHash lsh;
    private final WebTables webTables;
    private final KnowledgeIndex index;
    private final Set<MatchableTableRow> firstRecords;
    private final Set<MatchableTableRow> secondRecords;
    private final int columnIndex1;
    private final int columnIndex2;


    public LocalitySensitiveHashingBlockingKeyGenerator(WebTables webTables, KnowledgeIndex index, final Set<MatchableTableRow> firstRecords, final Set<MatchableTableRow> secondRecords, final int columnIndex1, final int columnIndex2) {
        this.webTables = webTables;
        this.index = index;
        this.firstRecords = firstRecords;
        this.secondRecords = secondRecords;
        this.columnIndex1 = columnIndex1;
        this.columnIndex2 = columnIndex2;
        final var universe = new HashSet<>(firstRecords);
        universe.addAll(secondRecords);
        this.universe = universe.toArray(MatchableTableRow[]::new);
        int numberOfBuckets = (int) Math.sqrt(this.universe.length);
        int stages = 15;

        lsh = new LSHMinHash(stages, numberOfBuckets, this.universe.length);
    }


    @Override
    public void generateBlockingKeys(final MatchableTableRow record, final Processable<Correspondence<MatchableValue, Matchable>> correspondences, final DataIterator<Pair<String, MatchableTableRow>> resultCollector) {
        final int key = getKey(record);
        final var matchableColumn = getMatchableColumn(record);
        resultCollector.next(new Pair<>(String.valueOf(key), record));
    }

    public int getKey(final MatchableTableRow record) {
        final var hash = lsh.hash(vectorize(record));
        return hash[hash.length - 1];
    }

    private MatchableTableColumn getMatchableColumn(final MatchableTableRow record) {
        if (firstRecords.contains(record)) {
            return index.findColumn(record.getTableId(), columnIndex1);
        } else {
            return webTables.findColumn(record.getTableId(), columnIndex2);
        }
    }

    private boolean[] vectorize(MatchableTableRow record) {
        var recordColIndex = firstRecords.contains(record) ? columnIndex1 : columnIndex2;

        final var vector = new boolean[universe.length];
        for (int i = 0; i < universe.length; i++) {

            var universeColIndex = firstRecords.contains(universe[i]) ? columnIndex1 : columnIndex2;

            final var universeValue = universe[i].get(universeColIndex);
            final var recordValue = record.get(recordColIndex);
            if (universeValue.equals(recordValue)) {
                vector[i] = true;
            }
        }
        return vector;
    }

}
