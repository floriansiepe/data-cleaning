package florian.siepe.entity.kb;

import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

public class MatchableLodColumn extends MatchableTableColumn {

    public static final int CSV_LENGTH = 2;
    private static final long serialVersionUID = 1L;
    private int globalId;

    public MatchableLodColumn() {

    }

    public MatchableLodColumn(final int tableId, final TableColumn c, final int globalId) {
        super(tableId, c);
        this.globalId = globalId;
        id = c.getIdentifier();
    }

    public static MatchableLodColumn fromCSV(final String[] values) {
        final MatchableLodColumn c = new MatchableLodColumn();

        c.tableId = -1;
        c.columnIndex = -1;
        c.type = DataType.valueOf(values[1]);
        c.id = values[0];

        return c;
    }

    /* (non-Javadoc)
     * @see de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn#toString()
     */
    @Override
    public String toString() {
        return String.format("%s", this.getIdentifier());
    }

}
