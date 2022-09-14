package florian.siepe.control;

import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.MatchableTableRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SampleExtractor {

    public <T, R> List<R> extractValue(final int k, final List<T> values, final Function<T, R> valueMapper) {
        if (k >= values.size()) {
            return values
                    .stream()
                    .map(valueMapper)
                    .filter(Objects::nonNull)
                    .sorted()
                    .collect(Collectors.toList());
        }

        var extractedValues = new ArrayList<R>(k);
        final double pickRatio = (double) values.size() / k;
        for (int i = 0; i < k; i++) {
            final int index = (int) (i * pickRatio);
            extractedValues.add(valueMapper.apply(values.get(index)));
        }

        return extractedValues;
    }

    public List<String> extractValue(final MatchableTableColumn column, final int k, final List<MatchableTableRow> values) {
        return this.extractValue(k, values, matchableTableRow -> {
            try {
                return String.valueOf(matchableTableRow.get(column.getColumnIndex()));
            } catch (final IndexOutOfBoundsException e) {
                // This can happen if a row is corrupted. Ignore value
                return null;
            }
        });
    }

}
