package florian.siepe.control;

import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.MatchableTableRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SampleExtractor {

    public <T, R> List<R> extractValue(int k, List<T> values, Function<T, R> valueMapper) {
        if (k >= values.size()) {
            return values
                    .stream()
                    .map(valueMapper)
                    .filter(Objects::nonNull)
                    .sorted()
                    .collect(Collectors.toList());
        }

        final var extractedValues = new ArrayList<R>(k);
        double pickRatio = (double) values.size() / k;
        for (int i = 0; i < k; i++) {
            int index = (int) (i * pickRatio);
            extractedValues.add(valueMapper.apply(values.get(index)));
        }

        return extractedValues;
    }

    public List<String> extractValue(MatchableTableColumn column, int k, List<MatchableTableRow> values) {
        return extractValue(k, values, matchableTableRow -> {
            try {
                return String.valueOf(matchableTableRow.get(column.getColumnIndex()));
            } catch (IndexOutOfBoundsException e) {
                // This can happen if a row is corrupted. Ignore value
                return null;
            }
        });
    }

}
