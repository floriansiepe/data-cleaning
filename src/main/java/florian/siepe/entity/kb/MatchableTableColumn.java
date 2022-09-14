package florian.siepe.entity.kb;

import de.uni_mannheim.informatik.dws.winter.model.Fusible;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumnStatistics;

import java.io.Serializable;
import java.util.Map;

public class MatchableTableColumn implements Matchable, Fusible<MatchableTableColumn>, Comparable<MatchableTableColumn>, Serializable {

	private static final long serialVersionUID = 1L;
	protected int tableId;
	protected int columnIndex;
	protected String id;
	protected DataType type;
	protected String header;
	protected TableColumnStatistics statistics;
	protected Object min, max;

	public MatchableTableColumn(final int tableId, final int columnIndex, final String header, final DataType type) {
		this.tableId = tableId;
		this.columnIndex = columnIndex;
		id = "";
		this.header = header;
		this.type = type;
	}

	public MatchableTableColumn(final int tableId, final TableColumn c) {
		this.tableId = tableId;
		columnIndex = c.getColumnIndex();
		type = c.getDataType();
		header = c.getHeader();

		// this controls the schema that we are matching to!
		// using c.getIdentifier() all dbp properties only exist once! (i.e. we cannot handle "_label" columns and the value of tableId is more or less random
		id = c.getUniqueName();
	}

	public static MatchableTableColumn fromCSV(final String[] values, final Map<String, Integer> tableIndices) {
		final MatchableTableColumn c = new MatchableTableColumn();
		c.tableId = tableIndices.get(values[0]);
		c.columnIndex = Integer.parseInt(values[1]);
		c.type = DataType.valueOf(values[2]);
		c.id = values[3];
		return c;
	}

	public int getTableId() {
		return this.tableId;
	}

	public int getColumnIndex() {
		return this.columnIndex;
	}

	protected void setColumnIndex(final int index) {
		this.columnIndex = index;
	}

	/**
	 * @return the header
	 */
	public String getHeader() {
		return this.header;
	}


	public static final int CSV_LENGTH = 4;

	/**
	 * @param header the header to set
	 */
	public void setHeader(final String header) {
		this.header = header;
	}

	public MatchableTableColumn() {

	}

	/**
	 * @return TableColumnStatistic the statistics
	 */
	public TableColumnStatistics getStatistics() {
		return this.statistics;
	}

	/**
	 * @param statistics the statistics to set
	 */
	public void setStatistics(final TableColumnStatistics statistics) {
		this.statistics = statistics;
	}

	@Override
	public String getIdentifier() {
		return this.id;
	}

	@Override
	public String getProvenance() {
		return null;
	}

	/**
	 * @return the type
	 */
	public DataType getType() {
		return this.type;
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.Fusable#hasValue(java.lang.Object)
	 */
	@Override
	public boolean hasValue(final MatchableTableColumn attribute) {
		return false;
	}

	/**
	 * @return the min
	 */
	public Object getMin() {
		return this.min;
	}

	/**
	 * @param min the min to set
	 */
	public void setMin(final Object min) {
		this.min = min;
	}

	/**
	 * @return the max
	 */
	public Object getMax() {
		return this.max;
	}

	/**
	 * @param max the max to set
	 */
	public void setMax(final Object max) {
		this.max = max;
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(final MatchableTableColumn o) {
		return id.compareTo(o.id);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return id.hashCode();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object obj) {
		if (obj instanceof MatchableTableColumn) {
			final MatchableTableColumn col = (MatchableTableColumn) obj;
			return id.equals(col.id);
		} else {
			return super.equals(obj);
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("{#%d}[%d]%s", tableId, columnIndex, header);
	}

}
