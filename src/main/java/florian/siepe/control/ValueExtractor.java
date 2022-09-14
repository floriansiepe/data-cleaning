package florian.siepe.control;

import florian.siepe.control.io.KnowledgeIndex;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.MatchableTableRow;
import florian.siepe.entity.kb.WebTables;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

public class ValueExtractor {
    private final KnowledgeIndex index;
    private final WebTables webTables;

    public ValueExtractor(KnowledgeIndex index, WebTables webTables) {
        this.index = index;
        this.webTables = webTables;
    }

    public Collection<MatchableTableRow> findIndexRecords(MatchableTableColumn attribute) {
        Optional<MatchableTableColumn> column = this.index.findColumn(attribute.getIdentifier()).stream().findFirst();
        if (column.isPresent()) {
            MatchableTableColumn matchableTableColumn = column.get();
            return this.index.getRecords().where(input -> input.getPropertyUriToColumnIndex().containsKey(matchableTableColumn.getIdentifier()))
                    .where(input -> null != input.get(input.getPropertyUriToColumnIndex().get(matchableTableColumn.getIdentifier())))
                    .get();
        }
        return Collections.emptyList();
    }

    public Collection<MatchableTableRow> findWebRecords(MatchableTableColumn column) {
        return this.webTables.getRecords().where(input -> input.hasColumn(column.getColumnIndex()) && input.getTableId() == column.getTableId()).get();
    }
}
