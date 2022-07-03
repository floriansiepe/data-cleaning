package florian.siepe.entity.kb;

import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

import java.io.Serializable;

/**
 * Model of a property from the knowledge base.
 *
 * @author Oliver Lehmberg (oli@dwslab.de)
 */
public class MatchableLodColumn extends MatchableTableColumn implements Serializable {

    public static final int CSV_LENGTH = 2;
    private static final long serialVersionUID = 1L;
    private int globalId;

    public MatchableLodColumn() {

    }

    public MatchableLodColumn(int tableId, TableColumn c, int globalId) {
        super(tableId, c);
        this.globalId = globalId;
        this.id = c.getIdentifier();
    }

    public static MatchableLodColumn fromCSV(String[] values) {
        MatchableLodColumn c = new MatchableLodColumn();

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
        return String.format("%s", getIdentifier());
    }

}
