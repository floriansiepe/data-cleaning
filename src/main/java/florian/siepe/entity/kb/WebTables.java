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

/**
 * Model of the Web Tables corpus that is matched.
 *
 * @author Oliver Lehmberg (oli@dwslab.de)
 */
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
    private boolean forceDetectKeys = false;
    private boolean convertValues = true;

    public static void setDoSerialise(boolean serialise) {
        doSerialise = serialise;
    }

    public static WebTables loadWebTables(List<File> webTables, boolean keepTablesInMemory, boolean convertValues, boolean forceDetectKeys) {
        if (webTables.isEmpty()) {
            logger.warn("No webtables found");
            return new WebTables();
        }

        final var tables = findFiles(webTables);

        WebTables web = new WebTables();
        web.setKeepTablesInMemory(keepTablesInMemory);
        web.setConvertValues(convertValues);
        web.setForceDetectKeys(forceDetectKeys);
        web.load(tables);

        return web;
    }

    public static WebTables deserialise(File location) {
        try (final var input = new Input(new FileInputStream(location))) {
            logger.info("Deserializing Web Tables");
            Kryo kryo = KryoFactory.createKryoInstance();
            return kryo.readObject(input, WebTables.class);

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * @param forceDetectKeys the forceDetectKeys to set
     */
    public void setForceDetectKeys(boolean forceDetectKeys) {
        this.forceDetectKeys = forceDetectKeys;
    }

    public void setKeepTablesInMemory(boolean keep) {
        if (keep) {
            tablesById = new HashMap<>();
        } else {
            tablesById = null;
        }
    }

    /**
     * @param convertValues the convertValues to set
     */
    public void setConvertValues(boolean convertValues) {
        this.convertValues = convertValues;
    }

    public void load(List<File> webFiles) {
        CsvTableParser csvParser = new CsvTableParser();
        JsonTableParser jsonParser = new JsonTableParser();

        jsonParser.setConvertValues(convertValues);

        ProgressReporter progress = new ProgressReporter(webFiles.size(), "Loading Web Tables");

        int tblIdx = 0;
        for (File f : webFiles) {
            try {
                Table web = null;

                if (f.getName().endsWith("csv")) {
                    web = csvParser.parseTable(f);
                } else if (f.getName().endsWith("json")) {
                    web = jsonParser.parseTable(f);
                } else {
                    System.out.println(String.format("Unknown table format: %s", f.getName()));
                }

                if (web == null) {
                    continue;
                }

                if (forceDetectKeys) {
                    web.identifySubjectColumn();
                }

                if (tablesById != null) {
                    tablesById.put(tblIdx, web);
                    web.setTableId(tblIdx);
                }

                MatchableTable mt = new MatchableTable(tblIdx, web.getPath());
                tables.add(mt);

                if (webFiles.size() == 1) {
                    printTableReport(web);
                }

                tableIndices.put(web.getPath(), tblIdx);
                tableNames.put(tblIdx, web.getPath());
                String url = "";
                if (web.getContext() != null) {
                    url = web.getContext().getUrl();
                }
                tableURLs.put(tblIdx, url);

                // list records
                for (TableRow r : web.getRows()) {
                    MatchableTableRow row = new MatchableTableRow(r, tblIdx);
                    records.add(row);
                }
                // list schema
                for (TableColumn c : web.getSchema().getRecords()) {
                    MatchableTableColumn mc = new MatchableTableColumn(tblIdx, c);
                    schema.add(mc);
                    columnHeaders.put(mc.getIdentifier(), c.getHeader());
                    if (c.getDataType() == DataType.numeric) {
                        mc.setStatistics(c.calculateColumnStatistics());
                    } else if (c.getDataType() == DataType.date) {
                        for (TableRow row : web.getRows()) {
                            LocalDateTime value = (LocalDateTime) row.get(c.getColumnIndex());
                            if (value != null) {
                                if (mc.getMax() == null || value.compareTo((LocalDateTime) mc.getMax()) > 0) {
                                    mc.setMax(value);
                                }
                                if (mc.getMin() == null || value.compareTo((LocalDateTime) mc.getMin()) < 0) {
                                    mc.setMin(value);
                                }
                            }
                        }
                    }
                    if (web.hasSubjectColumn() && web.getSubjectColumnIndex() == c.getColumnIndex()) {
                        //keys.put(mc.getTableId(), mc);
                        keyIndices.put(tblIdx, c.getColumnIndex());
                        keys.add(new Pair<Integer, MatchableTableColumn>(mc.getTableId(), mc));
                    }
                }
                tblIdx++;
            } catch (Exception e) {
                e.printStackTrace();
            }

            progress.incrementProgress();
            progress.report();
        }

        System.out.println(String.format("%,d Web Tables Instances loaded.", records.size()));
        System.out.println(String.format("%,d Web Tables Columns", schema.size()));
    }

    public DataSet<MatchableTableRow, MatchableTableColumn> getRecords() {
        return records;
    }

    public DataSet<MatchableTableColumn, MatchableTableColumn> getSchema() {
        return schema;
    }

    public DataSet<MatchableTable, MatchableTableColumn> getTables() {
        return tables;
    }

    public Processable<Pair<Integer, MatchableTableColumn>> getKeys() {
        return keys;
    }

    /**
     * @return A map (Column Identifier) -> (Column Header)
     */
    public HashMap<String, String> getColumnHeaders() {
        return columnHeaders;
    }

    /**
     * @return the tables
     */
    public HashMap<Integer, Table> getTablesById() {
        return tablesById;
    }

    /**
     * @return A map (Table Path) -> (Table Id)
     */
    public HashMap<String, Integer> getTableIndices() {
        return tableIndices;
    }

    /**
     * @return A map (Table Id) -> (Table Path)
     */
    public Map<Integer, String> getTableNames() {
        return tableNames;
    }

    /**
     * @return A map (Table Id) -> (Table URL)
     */
    public Map<Integer, String> getTableURLs() {
        return tableURLs;
    }

    /**
     * A map (Table ID) -> (Key Column Index)
     *
     * @return the keyIndices
     */
    public HashMap<Integer, Integer> getKeyIndices() {
        return keyIndices;
    }

    public void serialise(File location) {
        try (final var output = new Output(new FileOutputStream(location))) {
            System.out.println("Serialising Web Tables");

            Kryo kryo = KryoFactory.createKryoInstance();
            kryo.writeObject(output, this);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    public void printTableReport(Table t) {
        System.out.println(String.format("%s: %d columns, %d rows", t.getPath(), t.getColumns().size(), t.getRows().size()));
        for (TableColumn tc : t.getColumns()) {
            System.out.println(String.format("\t[%d] %s (%s) %s", tc.getColumnIndex(), tc.getHeader(), tc.getDataType(), tc.getColumnIndex() == t.getSubjectColumnIndex() ? " *entity label column*" : ""));
        }
    }

    public MatchableTableColumn findColumn(final int tableId, final int columnIndex2) {
        return schema.where(input -> input.getTableId() == tableId && input.getColumnIndex() == columnIndex2).firstOrNull();
    }
}
