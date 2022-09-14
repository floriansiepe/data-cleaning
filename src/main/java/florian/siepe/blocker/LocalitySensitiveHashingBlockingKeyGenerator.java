package florian.siepe.blocker;

import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.MatchableValue;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Function;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import florian.siepe.entity.kb.MatchableTableRow;
import info.debatty.java.lsh.LSHMinHash;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalitySensitiveHashingBlockingKeyGenerator extends BlockingKeyGenerator<MatchableTableRow, MatchableValue, MatchableTableRow> {
    private final MatchableTableRow[] universe;
    private final LSHMinHash lsh;
    private final Set<MatchableTableRow> firstRecords;
    private final Function<Object, MatchableTableRow> columnProvider1;
    private final Function<Object, MatchableTableRow> columnProvider2;


    public LocalitySensitiveHashingBlockingKeyGenerator(Set<MatchableTableRow> firstRecords, Set<MatchableTableRow> secondRecords, Function<Object, MatchableTableRow> columnProvider1, Function<Object, MatchableTableRow> columnProvider2) {
        this.firstRecords = firstRecords;
        this.columnProvider1 = columnProvider1;
        this.columnProvider2 = columnProvider2;
        var universe = new HashSet<>(firstRecords);
        universe.addAll(secondRecords);
        this.universe = universe.toArray(MatchableTableRow[]::new);
        final int numberOfBuckets = (int) Math.sqrt(this.universe.length);
        final int stages = 1 < numberOfBuckets ? (int) Math.ceil(Math.log(numberOfBuckets) / Math.log(2)) : 1;
        this.lsh = new LSHMinHash(stages, numberOfBuckets, this.universe.length);
    }


    @Override
    public void generateBlockingKeys(MatchableTableRow record, Processable<Correspondence<MatchableValue, Matchable>> correspondences, DataIterator<Pair<String, MatchableTableRow>> resultCollector) {
        int key = this.getKey(record);
        resultCollector.next(new Pair<>(String.valueOf(key), record));
    }

    public Stream<Integer> getKeys(MatchableTableRow record) {
        var hash = this.lsh.hash(this.vectorize(record));

        return Arrays.stream(hash)
                .boxed()
                .collect(Collectors.groupingBy(java.util.function.Function.identity(), Collectors.counting()))
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .limit(5);
    }

    public int getKey(MatchableTableRow record) {
        final var keys = this.getKeys(record).toArray(Integer[]::new);
        if (0 < keys.length) {
            return keys[0];
        }
        return -1;
    }

    private boolean[] vectorize(final MatchableTableRow record) {
        final var recordProvider = this.firstRecords.contains(record) ? this.columnProvider1 : this.columnProvider2;

        var vector = new boolean[this.universe.length];
        for (int i = 0; i < this.universe.length; i++) {

            final var universeProvider = this.firstRecords.contains(this.universe[i]) ? this.columnProvider1 : this.columnProvider2;

            var universeValue = universeProvider.execute(this.universe[i]);
            if (null == universeValue) {
                System.out.println();
            }
            var recordValue = recordProvider.execute(record);
            if (universeValue.equals(recordValue)) {
                vector[i] = true;
            }
        }
        return vector;
    }

}
