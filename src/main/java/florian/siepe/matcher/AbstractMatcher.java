package florian.siepe.matcher;

import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.HashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import florian.siepe.control.io.KnowledgeIndex;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.WebTables;

import java.util.HashMap;
import java.util.List;

import static java.util.stream.Collectors.groupingBy;

public abstract class AbstractMatcher {
    public HashMap<Integer, Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>>> runMatching(KnowledgeIndex index, WebTables webTables) {
        // Use the table id as key
        final var correspondences = new HashMap<Integer, Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>>>();

        final var webTableColumnsByTableId = webTables.getSchema().get().stream().collect(groupingBy(MatchableTableColumn::getTableId));

        for (final var entry : webTableColumnsByTableId.entrySet()) {
            final var webTableId = entry.getKey();
            final var columns = entry.getValue();
            correspondences.put(webTableId, runMatching(index, columns));
        }
        return correspondences;
    }

    private Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> runMatching(final KnowledgeIndex index, final List<MatchableTableColumn> columns) {
        final var webTableColumnSet = new HashedDataSet<MatchableTableColumn, MatchableTableColumn>(columns);
        return runMatching(index.getSchema(), webTableColumnSet, null);
    }

    public abstract Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> runMatching(DataSet<MatchableTableColumn, MatchableTableColumn> dataSet, DataSet<MatchableTableColumn, MatchableTableColumn> dataSet1, Processable<Correspondence<MatchableTableColumn, Matchable>> processable);
}
