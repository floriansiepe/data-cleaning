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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

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

    private final Map<Integer, Map<Integer, Double>> classSimilarities = new HashMap<>();

    public static HashMap<String, String> getClassHierarchy() {
        return classHierarchy;
    }

    public boolean hasProperty(String uri) {
        return propertyIds.containsKey(uri);
    }

    public void addProperty(final String uri, final int globalId) {
        propertyIds.put(uri, globalId);
    }

    public Integer globalPropertyId(String uri) {
        return propertyIds.get(uri);
    }

    public void addClassProperty(final int tableId, final BiMap<Integer, Integer> indexTranslation) {
        classProperties.put(tableId, indexTranslation);
    }

    public void addTable(final String className, final int tableId, final MatchableTable table) {
        tableIds.put(className, tableId);
        tables.add(table);
    }

    @Override
    public String toString() {
        return "KnowledgeIndex{" +
                "classTableIds=" + tableIds +
                ", propertyIds=" + propertyIds +
                ", tableIdToTable=" + tableIdToTable +
                ", classProperties=" + classProperties +
                '}';
    }

    public BiMap<String, Integer> getPropertyIds() {
        return propertyIds;
    }

    public BiMap<Integer, Table> getTableIdToTable() {
        return tableIdToTable;
    }

    public BiMap<Integer, BiMap<Integer, Integer>> getClassProperties() {
        return classProperties;
    }

    public BiMap<String, Integer> getTableIds() {
        return tableIds;
    }

    public void addClassSimilarity(final String frist, final String second, final double similarity) {
        final var firstId = tableIds.get(frist);
        final var secondId = tableIds.get(second);

        classSimilarities.computeIfAbsent(firstId, id -> {
            final var map = new HashMap<Integer, Double>();
            map.put(secondId, similarity);
            return map;
        });

        classSimilarities.computeIfPresent(firstId, (id, map) -> {
            map.put(secondId, similarity);
            return map;
        });

        classSimilarities.computeIfAbsent(secondId, id -> {
            final var map = new HashMap<Integer, Double>();
            map.put(firstId, similarity);
            return map;
        });

        classSimilarities.computeIfPresent(secondId, (id, map) -> {
            map.put(firstId, similarity);
            return map;
        });
    }

    public void addSchema(final MatchableLodColumn mc) {
        schema.add(mc);
    }

    public void addPropertyIndex(final int tblIdx, final BiMap<Integer, Integer> indexTranslation) {
        propertyIndices.put(tblIdx, indexTranslation);
    }

    public MatchableTableRow getRecord(final String identifier) {
        return records.getRecord(identifier);
    }

    public String getClassIndex(final int tableId) {
        return classIds.inverse().get(tableId);
    }

    public void addClassIndex(final int tblIdx, final String className) {
        classIds.put(className, tblIdx);
    }

    public String getSuperclass(final String clazz) {
        return classHierarchy.get(clazz);
    }

    public void addRecord(final MatchableTableRow mr) {
        records.add(mr);
    }

    public Set<String> getClasses() {
        return classIds.keySet();
    }

    public boolean hasClass(final String clazz) {
        return classIds.containsKey(clazz);
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
}
