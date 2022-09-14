package florian.siepe.entity.kb;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.FusibleParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.ParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.ProgressReporter;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.CsvTableParser;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.JsonTableParser;
import florian.siepe.control.io.KryoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static florian.siepe.utils.FileUtil.findFiles;

public class WebTables implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(WebTables.class);

    private static final long serialVersionUID = 1L;
    private static boolean doSerialise = true;
    // data that will be matched: records and schema
    private final DataSet<MatchableTableRow, MatchableTableColumn> records = new FusibleParallelHashedDataSet<>();
    private final DataSet<MatchableTableColumn, MatchableTableColumn> schema = new ParallelHashedDataSet<>();
    private final DataSet<MatchableTable, MatchableTableColumn> tables = new ParallelHashedDataSet<>();
    // matched web tables and their key columns
    private final Processable<Pair<Integer, MatchableTableColumn>> keys = new ParallelProcessableCollection<>();
    // translation for web table identifiers
    private final HashMap<String, String> columnHeaders = new HashMap<>();
    // translation from table name to table id
    private final HashMap<String, Integer> tableIndices = new HashMap<>();
    // translation from table id to table name
    private final Map<Integer, String> tableNames = new HashMap<>();
    // translation from table id to table URL
    private final Map<Integer, String> tableURLs = new HashMap<>();
    // lookup for key column
    private final HashMap<Integer, Integer> keyIndices = new HashMap<>();
    // lookup for tables by id
    private HashMap<Integer, Table> tablesById = new HashMap<>();
    // detect entity label columns, even if they are set in the file
    private boolean forceDetectKeys;
    private boolean convertValues = true;

    public static void setDoSerialise(final boolean serialise) {
        WebTables.doSerialise = serialise;
    }

    public static WebTables loadWebTables(final List<File> webTables, final boolean keepTablesInMemory, final boolean convertValues, final boolean forceDetectKeys) {
        if (webTables.isEmpty()) {
            WebTables.logger.warn("No webtables found");
            return new WebTables();
        }

        var tables = findFiles(webTables);

        final WebTables web = new WebTables();
        web.setKeepTablesInMemory(keepTablesInMemory);
        web.convertValues = convertValues;
        web.forceDetectKeys = forceDetectKeys;
        web.load(tables);

        return web;
    }

    public static WebTables deserialise(final File location) {
        try (var input = new Input(new FileInputStream(location))) {
            WebTables.logger.info("Deserializing Web Tables");
            final Kryo kryo = KryoFactory.createKryoInstance();
            return kryo.readObject(input, WebTables.class);

        } catch (final FileNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * @param forceDetectKeys the forceDetectKeys to set
     */
    public void setForceDetectKeys(final boolean forceDetectKeys) {
        this.forceDetectKeys = forceDetectKeys;
    }

    public void setKeepTablesInMemory(final boolean keep) {
        if (keep) {
            this.tablesById = new HashMap<>();
        } else {
            this.tablesById = null;
        }
    }

    /**
     * @param convertValues the convertValues to set
     */
    public void setConvertValues(final boolean convertValues) {
        this.convertValues = convertValues;
    }

    public void load(final List<File> webFiles) {
        final CsvTableParser csvParser = new CsvTableParser();
        final JsonTableParser jsonParser = new JsonTableParser();

        jsonParser.setConvertValues(this.convertValues);

        final ProgressReporter progress = new ProgressReporter(webFiles.size(), "Loading Web Tables");

        int tblIdx = 0;
        for (final File f : webFiles) {
            try {
                Table web = null;

                if (f.getName().endsWith("csv")) {
                    web = csvParser.parseTable(f);
                } else if (f.getName().endsWith("json")) {
                    web = jsonParser.parseTable(f);
                } else {
                    System.out.printf("Unknown table format: %s%n", f.getName());
                }

                if (null == web) {
                    continue;
                }

                if (this.forceDetectKeys) {
                    web.identifySubjectColumn();
                }

                if (null != tablesById) {
                    this.tablesById.put(tblIdx, web);
                    web.setTableId(tblIdx);
                }

                final MatchableTable mt = new MatchableTable(tblIdx, web.getPath());
                this.tables.add(mt);

                if (1 == webFiles.size()) {
                    this.printTableReport(web);
                }

                this.tableIndices.put(web.getPath(), tblIdx);
                this.tableNames.put(tblIdx, web.getPath());
                String url = "";
                if (null != web.getContext()) {
                    url = web.getContext().getUrl();
                }
                this.tableURLs.put(tblIdx, url);

                // list records
                for (final TableRow r : web.getRows()) {
                    final MatchableTableRow row = new MatchableTableRow(r, tblIdx);
                    this.records.add(row);
                }
                // list schema
                for (final TableColumn c : web.getSchema().getRecords()) {
                    final MatchableTableColumn mc = new MatchableTableColumn(tblIdx, c);
                    this.schema.add(mc);
                    this.columnHeaders.put(mc.getIdentifier(), c.getHeader());
                    if (DataType.numeric == c.getDataType()) {
                        mc.setStatistics(c.calculateColumnStatistics());
                    } else if (DataType.date == c.getDataType()) {
                        for (final TableRow row : web.getRows()) {
                            final LocalDateTime value = (LocalDateTime) row.get(c.getColumnIndex());
                            if (null != value) {
                                if (null == mc.getMax() || 0 < value.compareTo((LocalDateTime) mc.getMax())) {
                                    mc.setMax(value);
                                }
                                if (null == mc.getMin() || 0 > value.compareTo((LocalDateTime) mc.getMin())) {
                                    mc.setMin(value);
                                }
                            }
                        }
                    }
                    if (web.hasSubjectColumn() && web.getSubjectColumnIndex() == c.getColumnIndex()) {
                        //keys.put(mc.getTableId(), mc);
                        this.keyIndices.put(tblIdx, c.getColumnIndex());
                        this.keys.add(new Pair<Integer, MatchableTableColumn>(mc.getTableId(), mc));
                    }
                }
                tblIdx++;
            } catch (final Exception e) {
                e.printStackTrace();
            }

            progress.incrementProgress();
            progress.report();
        }

        System.out.printf("%,d Web Tables Instances loaded.%n", this.records.size());
        System.out.printf("%,d Web Tables Columns%n", this.schema.size());
    }

    public DataSet<MatchableTableRow, MatchableTableColumn> getRecords() {
        return this.records;
    }

    public DataSet<MatchableTableColumn, MatchableTableColumn> getSchema() {
        return this.schema;
    }

    public DataSet<MatchableTable, MatchableTableColumn> getTables() {
        return this.tables;
    }

    public Processable<Pair<Integer, MatchableTableColumn>> getKeys() {
        return this.keys;
    }

    /**
     * @return A map (Column Identifier) -> (Column Header)
     */
    public HashMap<String, String> getColumnHeaders() {
        return this.columnHeaders;
    }

    /**
     * @return the tables
     */
    public HashMap<Integer, Table> getTablesById() {
        return this.tablesById;
    }

    /**
     * @return A map (Table Path) -> (Table Id)
     */
    public HashMap<String, Integer> getTableIndices() {
        return this.tableIndices;
    }

    /**
     * @return A map (Table Id) -> (Table Path)
     */
    public Map<Integer, String> getTableNames() {
        return this.tableNames;
    }

    /**
     * @return A map (Table Id) -> (Table URL)
     */
    public Map<Integer, String> getTableURLs() {
        return this.tableURLs;
    }

    /**
     * A map (Table ID) -> (Key Column Index)
     *
     * @return the keyIndices
     */
    public HashMap<Integer, Integer> getKeyIndices() {
        return this.keyIndices;
    }

    public void serialise(final File location) {
        try (var output = new Output(new FileOutputStream(location))) {
            System.out.println("Serialising Web Tables");

            final Kryo kryo = KryoFactory.createKryoInstance();
            kryo.writeObject(output, this);
        } catch (final FileNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    public void printTableReport(final Table t) {
        System.out.printf("%s: %d columns, %d rows%n", t.getPath(), t.getColumns().size(), t.getRows().size());
        for (final TableColumn tc : t.getColumns()) {
            System.out.printf("\t[%d] %s (%s) %s%n", tc.getColumnIndex(), tc.getHeader(), tc.getDataType(), tc.getColumnIndex() == t.getSubjectColumnIndex() ? " *entity label column*" : "");
        }
    }

    public MatchableTableColumn findColumn(int tableId, int columnIndex2) {
        return this.schema.where(input -> input.getTableId() == tableId && input.getColumnIndex() == columnIndex2).firstOrNull();
    }
}
