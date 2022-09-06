package florian.siepe.control.io;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.lod.LodTableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.LodCsvTableParser;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.TableParser;
import florian.siepe.control.nlp.CamelCaseTokenizer;
import florian.siepe.control.nlp.Word2VecFactory;
import florian.siepe.entity.kb.MatchableLodColumn;
import florian.siepe.entity.kb.MatchableTable;
import florian.siepe.entity.kb.MatchableTableRow;
import florian.siepe.t2k.DBpediaIndexer;
import florian.siepe.t2k.SurfaceForms;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.deeplearning4j.text.tokenization.tokenizer.Tokenizer;
import org.jgrapht.alg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

import static florian.siepe.utils.FileUtil.findFiles;

public class KnowledgeBase implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(KnowledgeBase.class);
    private static final long serialVersionUID = 1L;
    private static final String KB_PATH = "data/kb.bin";
    private static KnowledgeBase kb;
    private final GlobalIdProvider globalIdProvider = new GlobalIdProvider();
    private final TableParser lodParser;
    private final KnowledgeIndex knowledgeIndex = new KnowledgeIndex();

    public KnowledgeBase() {
        this(new LodCsvTableParser());
    }

    public KnowledgeBase(final TableParser lodParser) {
        this.lodParser = lodParser;
    }

    public synchronized static KnowledgeBase getInstance(List<File> kbFiles, IIndex index, SurfaceForms sForms) {
        if (kb != null) {
            return kb;
        }

        File f = new File(KB_PATH);
        if (f.exists() && f.isFile()) {
            kb = deserialize();
            return kb;
        }

        kb = new KnowledgeBase();
        kb.index(kbFiles, index, sForms);
        return kb;
    }

    public static KnowledgeBase deserialize() {
        logger.info("Deserializing Knowledge Base");

        try (Input input = new Input(new FileInputStream(KB_PATH))) {
            Kryo kryo = KryoFactory.createKryoInstance();
            return kryo.readObject(input, KnowledgeBase.class);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    void index(List<File> files, IIndex index, SurfaceForms sForms) {
        Integer tableId = 0;
        final List<File> kbFiles = findFiles(files);


        for (File file : kbFiles) {
            final int finalTableId = tableId;
                logger.info("Loading table {}", file.getName());
                final var table = lodParser.parseTable(file);
                table.setTableId(finalTableId);
                final var className = computeClassnameFromFileName(table.getPath());
                MatchableTable mt = new MatchableTable(finalTableId, className);
                knowledgeIndex.addTable(className, finalTableId, mt);

                if (hasLabelAsKey(table)) {
                    table.setSubjectColumnIndex(1);

                    logger.info("Found the following columns for class [{}] in table [{}]", className, file.getPath());
                    for (final TableColumn column : table.getSchema().getRecords()) {
                        logger.info(String.format("{%s} [%d] %s (%s): %s", table.getPath(), column.getColumnIndex(), column.getHeader(), column.getDataType(), column.getUri()));
                    }

                    cleanUp(table);
                    indexTableSchema(table);
                }
            tableId++;
        }


        addMissingClasses(tableId);
        //computeClassSimilarities();

        LodCsvTableParser.endLoadData();
        calculateClassWeight();

        if(index!=null) {
            System.out.println("Indexing ...");
            DBpediaIndexer indexer = new DBpediaIndexer();
            indexer.indexInstances(index, knowledgeIndex.getRecords().get(), knowledgeIndex.getClassIds().inverse(), sForms);
        }

        serialize();
    }

    private void addMissingClasses(Integer tableId) {
        // add classes from the class hierarchy which have not been loaded (but can be mapped via the hierarchy)
        for (String cls : new HashSet<>(knowledgeIndex.getClasses())) {

            String superClass = knowledgeIndex.getSuperclass(cls);

            while (superClass != null) {

                if (!knowledgeIndex.hasClass(superClass)) {
                    MatchableTable mt = new MatchableTable(tableId, superClass);
                    knowledgeIndex.addTable(superClass, tableId, mt);
                    tableId++;
                }

                superClass = knowledgeIndex.getSuperclass(superClass);
            }

        }
    }

    @Deprecated
    void computeClassSimilarities() {
        final var word2Vec = Word2VecFactory.getInstance();
        final var classes = knowledgeIndex.getTableIds().keySet();

        final var computedClassPairs = new HashSet<Pair<String, String>>();

        for (final String clazz : classes) {
            for (final String innerClass : classes) {
                final var pair = Pair.of(clazz, innerClass);
                if (innerClass.equals(clazz) || computedClassPairs.contains(pair)) {
                    continue;
                }
                computedClassPairs.add(pair);

                var frist = normalizeClassName(clazz);
                var second = normalizeClassName(innerClass);

                final var similarity = word2Vec.calculate(frist, second);
                knowledgeIndex.addClassSimilarity(clazz, innerClass, similarity);
            }
        }
    }

    public void calculateClassWeight(){
        double max = -1;

        for (Map.Entry<Integer, Integer> tableSize : knowledgeIndex.getSizePerTable().entrySet()) {
            if (tableSize.getValue() < 1) {
                continue;
            }
            if (tableSize.getValue() > max) {
                max = tableSize.getValue();
            }
        }

        for(Map.Entry<Integer, Integer> tableSize : knowledgeIndex.getSizePerTable().entrySet()){
            double value = 0;
            if (tableSize.getValue() < 1) {
                value = 1;
            }
            value =tableSize.getValue()/max;
            value = 1-value;
            knowledgeIndex.getClassWeight().put(tableSize.getKey(), value);
        }


    }

    String normalizeClassName(final String clazz) {
        Tokenizer tokenizer = new CamelCaseTokenizer(clazz);
        tokenizer.setTokenPreProcessor(String::toLowerCase);
        final var sb = new StringBuilder();
        for (var token : tokenizer.getTokens()) {
            sb.append(token).append(" ");
        }

        return sb.toString().trim();
    }

    void indexTableSchema(final Table table) {
        int colIdx = 0;
        BiMap<Integer, Integer> indexTranslation = HashBiMap.create();
        for (TableColumn tc : table.getSchema().getRecords()) {
            if (!knowledgeIndex.hasProperty(tc.getUri())) {
                int globalId = knowledgeIndex.getProperties().size();
                knowledgeIndex.addProperty(tc.getUri(), globalId);
                MatchableLodColumn mc = new MatchableLodColumn(0, tc, globalId);
                knowledgeIndex.addSchema(mc);

                indexTranslation.put(globalId, colIdx);
                if(tc.getUri().equals("http://www.w3.org/2000/01/rdf-schema#label")) {
                    knowledgeIndex.setRdfsLabel(mc);
                }

            } else {
                // The property with uri has already been indexed. Add the mapping only
                Integer globalPropertyId = knowledgeIndex.globalPropertyId(tc.getUri());
                // translate DBpedia-wide id (globalPropertyId) to index in type and value array (colIdx)
                // (indexTranslation is per table)
                indexTranslation.put(globalPropertyId, colIdx);
            }

            colIdx++;
        }
        knowledgeIndex.addClassProperty(table.getTableId(), indexTranslation);

        //###
        int tblIdx = table.getTableId();
        if (tblIdx == 0) {
            System.out.println(table.getPath());
        }
        knowledgeIndex.addPropertyIndex(tblIdx, indexTranslation);

        for (TableRow r : table.getRows()) {
            // make sure only the instance with the most specific class (=largest number of columns) remains in the final dataset for each URI
            MatchableTableRow mr = knowledgeIndex.getRecord(r.getIdentifier());

            if (mr == null) {
                mr = new MatchableTableRow(r, tblIdx);
            } else {
                String clsOfPrevoisRecord = knowledgeIndex.getClassIndex(mr.getTableId());
                String clsOfCurrentRecord = computeClassnameFromFileName(table.getPath());

                if (knowledgeIndex.getSuperclass(clsOfPrevoisRecord) == null) {
                    continue;
                } else {
                    String cls;
                    boolean flag = false;
                    while ((cls = knowledgeIndex.getSuperclass(clsOfPrevoisRecord)) != null) {
                        if (cls.equals(clsOfCurrentRecord)) {
                            flag = true;
                            break;
                        } else {
                            clsOfPrevoisRecord = cls;
                        }
                    }
                    if (flag == false) {
                        mr = new MatchableTableRow(r, tblIdx);
                    }
                }

            }

            knowledgeIndex.addRecord(mr);
        }
        knowledgeIndex.getSizePerTable().put(tblIdx, table.getSize());
        knowledgeIndex.addClassIndex(tblIdx, computeClassnameFromFileName(table.getPath()));

    }

    void cleanUp(final Table table) {
        // remove object properties and keep only "_label" columns (otherwise we will have duplicate property URLs) LodTableColumn[] cols = table.getColumns().toArray(new LodTableColumn[table.getSchema().getSize()]);
        LodTableColumn[] cols = table.getColumns().toArray(new LodTableColumn[table.getSchema().getSize()]);
        List<Integer> removedColumns = new LinkedList<>();
        for (LodTableColumn tc : cols) {
            if (tc.isReferenceLabel()) {
                Iterator<TableColumn> it = table.getSchema().getRecords().iterator();

                while (it.hasNext()) {
                    LodTableColumn ltc = (LodTableColumn) it.next();

                    if (!ltc.isReferenceLabel() && ltc.getUri().equals(tc.getUri())) {
                        it.remove();
                        removedColumns.add(ltc.getColumnIndex());
                    }
                }
            }
        }
        // re-create value arrays
        for (TableRow r : table.getRows()) {
            Object[] values = new Object[table.getSchema().getSize()];

            int newIndex = 0;
            for (int i = 0; i < r.getValueArray().length; i++) {
                if (!removedColumns.contains(i)) {
                    values[newIndex++] = r.getValueArray()[i];
                }
            }

            r.set(values);
        }
    }

    /**
     * @param table Table to match
     * @return True if it's a complex schema and the first columns is the rdf label column
     */
    boolean hasLabelAsKey(final Table table) {
        return table.getSchema().getSize() > 1 && "rdf-schema#label".equals(table.getSchema().get(1).getHeader());
    }

    String computeClassnameFromFileName(String filename) {
        return filename.replace(".csv", "").replace(".gz", "");
    }

    public void serialize() {
        logger.info("Serializing Knowledge Base");

        try (Output output = new Output(new FileOutputStream(KB_PATH))) {
            Kryo kryo = KryoFactory.createKryoInstance();
            kryo.writeObject(output, this);
            logger.info("Serialized Knowledge Base");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public KnowledgeIndex getKnowledgeIndex() {
        return knowledgeIndex;
    }

}
