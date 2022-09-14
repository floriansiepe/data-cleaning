package florian.siepe.entity.kb;

import de.uni_mannheim.informatik.dws.winter.model.Fusible;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.utils.SparseArray;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

import java.io.Serializable;
import java.util.*;

public class MatchableTableRow implements Matchable, Fusible<MatchableTableColumn>, Serializable {

	private static final long serialVersionUID = 1L;

	public MatchableTableRow() {

	}

	private Map<String, Integer> propertyUriToColumnIndex = new HashMap<>();

	public MatchableTableRow(final String id) {
		this.id = id;
		tableId = -1;
		rowNumber = -1;
	}

	public MatchableTableRow(final String id, final Object[] values, final int tableId, final DataType[] types) {
		this.id = id;
		this.tableId = tableId;
		rowNumber = -1;

		indices = new int[values.length];
		final SparseArray<Object> valuesSparse = new SparseArray<>(values);
		this.values = valuesSparse.getValues();
		for (int i = 0; i < values.length; i++) {
			this.indices[i] = i;
		}

		this.types = new DataType[values.length];
		System.arraycopy(types, 0, this.types, 0, this.indices.length);
	}

	protected String id;
	private DataType[] types;
	private Object[] values;
	private int[] indices;
	private int rowNumber;
	private int tableId;
	private int rowLength; // total number of columns (including null values)

	public MatchableTableRow(final TableRow row, final int tableId) {
		this.tableId = tableId;
		rowNumber = row.getRowNumber();
		id = row.getIdentifier();
		rowLength = row.getTable().getSchema().getSize();

		final ArrayList<DataType> types = new ArrayList<>();
		final ArrayList<TableColumn> cols = new ArrayList<>(row.getTable().getSchema().getRecords());
		Collections.sort(cols, new TableColumn.TableColumnByIndexComparator());
		for (final TableColumn c : cols) {
			types.add(c.getDataType());
		}

		if (types.size() < row.getValueArray().length) {
			System.err.println("problem");
		}

		final SparseArray<Object> valuesSparse = new SparseArray<>(row.getValueArray());
		values = valuesSparse.getValues();
		indices = valuesSparse.getIndices();

		this.types = new DataType[this.values.length];
		for (int i = 0; i < this.indices.length; i++) {
			this.types[i] = types.get(this.indices[i]);
		}
	}

	@Override
	public String getIdentifier() {
		return this.id;
	}

	@Override
	public String getProvenance() {
		return null;
	}

	public int getNumCells() {
		return this.values.length;
	}

	public Object get(final int columnIndex) {
		if (null != indices) {
			return SparseArray.get(columnIndex, this.values, this.indices);
		} else {
			return this.values[columnIndex];
		}
	}

	/**
	 * Sets the respective value. If the value didn't exist before, the sparse representation is replaced by a dense representation, which can lead to higher memory consumption, but is faster when setting multiple values
	 *
	 * @param columnIndex
	 * @param value
	 */
	public void set(final int columnIndex, final Object value) {
		int maxLen = columnIndex + 1;

		if (null != indices) {
			maxLen = Math.max(maxLen, this.indices[this.indices.length - 1] + 1);

			final Object[] allValues = new Object[maxLen];
			for (int i = 0; i < this.indices.length; i++) {
				allValues[this.indices[i]] = this.values[i];
			}

			this.values = allValues;
			this.indices = null;
		} else {
			if (maxLen > this.values.length) {
				this.values = Arrays.copyOf(this.values, maxLen);
			}
		}

		this.values[columnIndex] = value;
	}

	public DataType getType(final int columnIndex) {
		if (null != indices) {
			final int idx = SparseArray.translateIndex(columnIndex, this.indices);

			if (-1 == idx) {
				return null;
			} else {
				return this.types[idx];
			}
		} else {
			return this.types[columnIndex];
		}
	}
	public Object[] getValues() {
		return this.values;
	}
	public DataType[] getTypes() {
		return this.types;
	}
	public int getRowNumber() {
		return this.rowNumber;
	}
	public int getTableId() {
		return this.tableId;
	}

	/**
	 * @return the rowLength
	 */
	public int getRowLength() {
		return this.rowLength;
	}

	public boolean hasColumn(final int columnIndex) {
		if (null != indices) {
			final int idx = SparseArray.translateIndex(columnIndex, this.indices);

			return -1 != idx;
		} else {
			return columnIndex < this.values.length;
		}
	}


	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.Fusable#hasValue(java.lang.String)
	 */
	@Override
	public boolean hasValue(final MatchableTableColumn attribute) {
		return this.hasColumn(attribute.getColumnIndex()) && null != get(attribute.getColumnIndex());
	}

	public String format(final int columnWidth) {
		final StringBuilder sb = new StringBuilder();

		boolean first = true;
		for (int i = 0; i < this.rowLength; i++) {

			if (!first) {
				sb.append(" | ");
			}

			final String value;
			if (this.hasColumn(i)) {
				final Object v = this.get(i);
				value = v.toString();
			} else {
				value = "null";
			}

			sb.append(this.padRight(value, columnWidth));

			first = false;
		}

		return sb.toString();
	}

	protected String padRight(String s, final int n) {
		if (0 == n) {
			return "";
		}
		if (s.length() > n) {
			s = s.substring(0, n);
		}
		s = s.replace("\n", " ");
		return String.format("%1$-" + n + "s", s);
	}

	public Map<String, Integer> getPropertyUriToColumnIndex() {
		return this.propertyUriToColumnIndex;
	}

	public void setPropertyUriToColumnIndex(Map<String, Integer> propertyUriToColumnIndex) {
		this.propertyUriToColumnIndex = propertyUriToColumnIndex;
	}
}
