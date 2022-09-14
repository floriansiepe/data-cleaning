package florian.siepe.control.io;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.ParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import florian.siepe.entity.kb.MatchableLodColumn;
import florian.siepe.entity.kb.MatchableTable;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.MatchableTableRow;

import java.io.Serializable;
import java.util.*;

public class KnowledgeIndex implements Serializable {
    //class hierarchy mapping class -> super class
    private static final HashMap<String, String> classHierarchy = new HashMap<String, String>();
    // data that will be matched: records and schema
    private final DataSet<MatchableTableRow, MatchableTableColumn> records = new ParallelHashedDataSet<>();
    private final DataSet<MatchableTableColumn, MatchableTableColumn> schema = new ParallelHashedDataSet<>();
    private final DataSet<MatchableTable, MatchableTableColumn> tables = new ParallelHashedDataSet<>();
    // translation from class name to table id
    private final BiMap<String, Integer> tableIds = HashBiMap.create();
    // translation from class name to table id
    private final BiMap<String, Integer> classIds = HashBiMap.create();
    private final BiMap<String, Integer> propertyIds = HashBiMap.create();
    // translation for DBpedia property URIs: URI string to integer
    private final LinkedList<String> properties = new LinkedList<>();
    private final BiMap<Integer, Table> tableIdToTable = HashBiMap.create();
    private final BiMap<Integer, BiMap<Integer, Integer>> classProperties = HashBiMap.create();
    // translation from property id to column index per DBpedia class
    private final BiMap<Integer, BiMap<Integer, Integer>> propertyIndices = HashBiMap.create();

    // translation from property id to class id to column index where this property is contained
    private final BiMap<Integer, Map<Integer, Integer>> propertyClassIndices = HashBiMap.create();

    private final Map<Integer, Map<Integer, Double>> classSimilarities = new HashMap<>();

    // rdfs:label
    private MatchableLodColumn rdfsLabel;
    private final HashMap<Integer, Double> classWeight = new HashMap<>();
    // lookup for tables by id
    private final HashMap<Integer, Integer> sizePerTable = new HashMap<>();

    public boolean hasProperty(final String uri) {
        return this.propertyIds.containsKey(uri);
    }

    public void addProperty(String uri, int globalId) {
        this.properties.add(uri);
        this.propertyIds.put(uri, globalId);
    }

    public Integer globalPropertyId(final String uri) {
        return this.propertyIds.get(uri);
    }

    public void addClassProperty(int tableId, BiMap<Integer, Integer> indexTranslation) {
        this.classProperties.put(tableId, indexTranslation);
    }

    public void addTable(String className, int tableId, MatchableTable table) {
        this.tableIds.put(className, tableId);
        this.tables.add(table);
    }

    @Override
    public String toString() {
        return "KnowledgeIndex{" +
                "classTableIds=" + this.tableIds +
                ", propertyIds=" + this.propertyIds +
                ", tableIdToTable=" + this.tableIdToTable +
                ", classProperties=" + this.classProperties +
                '}';
    }


    public BiMap<String, Integer> getTableIds() {
        return this.tableIds;
    }

    public void addClassSimilarity(String frist, String second, double similarity) {
        var firstId = this.tableIds.get(frist);
        var secondId = this.tableIds.get(second);

        this.classSimilarities.computeIfAbsent(firstId, id -> {
            var map = new HashMap<Integer, Double>();
            map.put(secondId, similarity);
            return map;
        });

        this.classSimilarities.computeIfPresent(firstId, (id, map) -> {
            map.put(secondId, similarity);
            return map;
        });

        this.classSimilarities.computeIfAbsent(secondId, id -> {
            var map = new HashMap<Integer, Double>();
            map.put(firstId, similarity);
            return map;
        });

        this.classSimilarities.computeIfPresent(secondId, (id, map) -> {
            map.put(firstId, similarity);
            return map;
        });
    }

    public void addSchema(MatchableLodColumn mc) {
        this.schema.add(mc);
    }

    public void addPropertyIndex(int tblIdx, BiMap<Integer, Integer> indexTranslation) {
        this.propertyIndices.put(tblIdx, indexTranslation);
        var propertyIds = indexTranslation.keySet();
        for (Integer propertyId : propertyIds) {
            var columnIndex = indexTranslation.get(propertyId);
            this.propertyClassIndices.putIfAbsent(propertyId, new HashMap<>());
            this.propertyClassIndices.get(propertyId).putIfAbsent(tblIdx, columnIndex);
        }
    }

    public MatchableTableRow getRecord(String identifier) {
        return this.records.getRecord(identifier);
    }

    public String getClassIndex(int tableId) {
        return this.classIds.inverse().get(tableId);
    }

    public void addClassIndex(int tblIdx, String className) {
        this.classIds.put(className, tblIdx);
    }

    public String getSuperclass(String clazz) {
        return KnowledgeIndex.classHierarchy.get(clazz);
    }

    public void addRecord(MatchableTableRow mr) {
        this.records.add(mr);
    }

    public Set<String> getClasses() {
        return this.classIds.keySet();
    }

    public boolean hasClass(String clazz) {
        return this.classIds.containsKey(clazz);
    }

    public DataSet<MatchableTableRow, MatchableTableColumn> getRecords() {
        return this.records;
    }

    public DataSet<MatchableTableColumn, MatchableTableColumn> getSchema() {
        return this.schema;
    }

    public LinkedList<String> getProperties() {
        return this.properties;
    }

    public DataSet<MatchableTable, MatchableTableColumn> getTables() {
        return this.tables;
    }

    public Collection<Object> findColumnValues(String identifier) {
        Optional<MatchableTableColumn> column = this.findColumn(identifier).stream().findFirst();
        if (column.isPresent()) {
            MatchableTableColumn matchableTableColumn = column.get();

            return this.records.where(input -> input.getPropertyUriToColumnIndex().containsKey(matchableTableColumn.getIdentifier())).map(rec -> rec.get(rec.getPropertyUriToColumnIndex().get(matchableTableColumn.getIdentifier()))).get();
        }
        return Collections.emptyList();
    }

    public Collection<MatchableTableColumn> findColumn(String identifier) {
        return this.schema.where(input -> input.getIdentifier().equals(identifier)).get();
    }

    public MatchableLodColumn getRdfsLabel() {
        return this.rdfsLabel;
    }

    public void setRdfsLabel(MatchableLodColumn rdfsLabel) {
        this.rdfsLabel = rdfsLabel;
    }

    public HashMap<Integer, Double> getClassWeight() {
        return this.classWeight;
    }

    public HashMap<Integer, Integer> getSizePerTable() {
        return this.sizePerTable;
    }
}
