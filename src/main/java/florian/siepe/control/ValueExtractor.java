package florian.siepe.control;

import florian.siepe.control.io.KnowledgeIndex;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.MatchableTableRow;
import florian.siepe.entity.kb.WebTables;

import java.util.Collection;

public class ValueExtractor {
    private KnowledgeIndex index;
    private WebTables webTables;

    public ValueExtractor(final KnowledgeIndex index, final WebTables webTables) {
        this.index = index;
        this.webTables = webTables;
    }

    public Collection<MatchableTableRow> findIndexRecords(final MatchableTableColumn column) {
        if (column.getColumnIndex() == 23) {
            System.out.println();
        }

        return index.getRecords().where(input -> input.hasColumn(column.getColumnIndex()) && input.getTableId() == column.getTableId()).get();
    }

    public Collection<MatchableTableRow> findWebRecords(final MatchableTableColumn column) {
        return webTables.getRecords().where(input -> input.hasColumn(column.getColumnIndex()) && input.getTableId() == column.getTableId()).get();
    }
}
