package florian.siepe.control.io;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
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
    private final TableParser lodParser;
    private final KnowledgeIndex knowledgeIndex = new KnowledgeIndex();

    public KnowledgeBase() {
        this(new LodCsvTableParser());
    }

    public KnowledgeBase(TableParser lodParser) {
        this.lodParser = lodParser;
    }

    public static synchronized KnowledgeBase getInstance(final List<File> kbFiles) {
        if (null != kb) {
            return KnowledgeBase.kb;
        }

        final File f = new File(KnowledgeBase.KB_PATH);
        if (f.exists() && f.isFile()) {
            KnowledgeBase.kb = KnowledgeBase.deserialize();
            return KnowledgeBase.kb;
        }

        KnowledgeBase.kb = new KnowledgeBase();
        KnowledgeBase.kb.index(kbFiles);
        return KnowledgeBase.kb;
    }

    public static KnowledgeBase deserialize() {
        KnowledgeBase.logger.info("Deserializing Knowledge Base");

        try (final Input input = new Input(new FileInputStream(KnowledgeBase.KB_PATH))) {
            final Kryo kryo = KryoFactory.createKryoInstance();
            return kryo.readObject(input, KnowledgeBase.class);
        } catch (final FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    void index(final List<File> files) {
        Integer tableId = 0;
        List<File> kbFiles = findFiles(files);


        for (final File file : kbFiles) {
            int finalTableId = tableId;
            KnowledgeBase.logger.info("Loading table {}", file.getName());
            var table = this.lodParser.parseTable(file);
            table.setTableId(finalTableId);
            var className = this.computeClassnameFromFileName(table.getPath());
            final MatchableTable mt = new MatchableTable(finalTableId, className);
            this.knowledgeIndex.addTable(className, finalTableId, mt);

            if (this.hasLabelAsKey(table)) {
                table.setSubjectColumnIndex(1);

                KnowledgeBase.logger.info("Found the following columns for class [{}] in table [{}]", className, file.getPath());
                for (TableColumn column : table.getSchema().getRecords()) {
                    KnowledgeBase.logger.info(String.format("{%s} [%d] %s (%s): %s", table.getPath(), column.getColumnIndex(), column.getHeader(), column.getDataType(), column.getUri()));
                }

                this.cleanUp(table);
                this.indexTableSchema(table);
            }
            tableId++;
        }


        this.addMissingClasses(tableId);
        //computeClassSimilarities();

        LodCsvTableParser.endLoadData();
        this.calculateClassWeight();

        this.serialize();
    }

    private void addMissingClasses(Integer tableId) {
        // add classes from the class hierarchy which have not been loaded (but can be mapped via the hierarchy)
        for (final String cls : new HashSet<>(this.knowledgeIndex.getClasses())) {

            String superClass = this.knowledgeIndex.getSuperclass(cls);

            while (null != superClass) {

                if (!this.knowledgeIndex.hasClass(superClass)) {
                    final MatchableTable mt = new MatchableTable(tableId, superClass);
                    this.knowledgeIndex.addTable(superClass, tableId, mt);
                    tableId++;
                }

                superClass = this.knowledgeIndex.getSuperclass(superClass);
            }

        }
    }

    @Deprecated
    void computeClassSimilarities() {
        var word2Vec = Word2VecFactory.getInstance();
        var classes = this.knowledgeIndex.getTableIds().keySet();

        var computedClassPairs = new HashSet<Pair<String, String>>();

        for (String clazz : classes) {
            for (String innerClass : classes) {
                var pair = Pair.of(clazz, innerClass);
                if (innerClass.equals(clazz) || computedClassPairs.contains(pair)) {
                    continue;
                }
                computedClassPairs.add(pair);

                final var frist = this.normalizeClassName(clazz);
                final var second = this.normalizeClassName(innerClass);

                var similarity = word2Vec.calculate(frist, second);
                this.knowledgeIndex.addClassSimilarity(clazz, innerClass, similarity);
            }
        }
    }

    public void calculateClassWeight() {
        double max = -1;

        for (final Map.Entry<Integer, Integer> tableSize : this.knowledgeIndex.getSizePerTable().entrySet()) {
            if (1 > tableSize.getValue()) {
                continue;
            }
            if (tableSize.getValue() > max) {
                max = tableSize.getValue();
            }
        }

        for (final Map.Entry<Integer, Integer> tableSize : this.knowledgeIndex.getSizePerTable().entrySet()) {
            double value = 0;
            if (1 > tableSize.getValue()) {
                value = 1;
            }
            value = tableSize.getValue() / max;
            value = 1 - value;
            this.knowledgeIndex.getClassWeight().put(tableSize.getKey(), value);
        }


    }

    String normalizeClassName(String clazz) {
        final Tokenizer tokenizer = new CamelCaseTokenizer(clazz);
        tokenizer.setTokenPreProcessor(String::toLowerCase);
        var sb = new StringBuilder();
        for (final var token : tokenizer.getTokens()) {
            sb.append(token).append(" ");
        }

        return sb.toString().trim();
    }

    void indexTableSchema(Table table) {
        int colIdx = 0;
        final BiMap<Integer, Integer> indexTranslation = HashBiMap.create();
        HashMap<String, Integer> propertyUriToIndex = new HashMap<>();
        for (final TableColumn tc : table.getSchema().getRecords()) {
            if (!this.knowledgeIndex.hasProperty(tc.getUri())) {
                final int globalId = this.knowledgeIndex.getProperties().size();
                this.knowledgeIndex.addProperty(tc.getUri(), globalId);
                final MatchableLodColumn mc = new MatchableLodColumn(0, tc, globalId);
                this.knowledgeIndex.addSchema(mc);
                propertyUriToIndex.put(tc.getUri(), colIdx);

                indexTranslation.put(globalId, colIdx);
                if ("http://www.w3.org/2000/01/rdf-schema#label".equals(tc.getUri())) {
                    this.knowledgeIndex.setRdfsLabel(mc);
                }

            } else {
                // The property with uri has already been indexed. Add the mapping only
                final Integer globalPropertyId = this.knowledgeIndex.globalPropertyId(tc.getUri());
                // translate DBpedia-wide id (globalPropertyId) to index in type and value array (colIdx)
                // (indexTranslation is per table)
                indexTranslation.put(globalPropertyId, colIdx);
            }

            colIdx++;
        }
        this.knowledgeIndex.addClassProperty(table.getTableId(), indexTranslation);

        //###
        final int tblIdx = table.getTableId();
        if (0 == tblIdx) {
            System.out.println(table.getPath());
        }
        this.knowledgeIndex.addPropertyIndex(tblIdx, indexTranslation);

        for (final TableRow r : table.getRows()) {
            // make sure only the instance with the most specific class (=largest number of columns) remains in the final dataset for each URI
            MatchableTableRow mr = this.knowledgeIndex.getRecord(r.getIdentifier());

            if (null == mr) {
                mr = new MatchableTableRow(r, tblIdx);
            } else {
                String clsOfPrevoisRecord = this.knowledgeIndex.getClassIndex(mr.getTableId());
                final String clsOfCurrentRecord = this.computeClassnameFromFileName(table.getPath());

                if (null == knowledgeIndex.getSuperclass(clsOfPrevoisRecord)) {
                    continue;
                } else {
                    String cls;
                    boolean flag = false;
                    while (null != (cls = knowledgeIndex.getSuperclass(clsOfPrevoisRecord))) {
                        if (cls.equals(clsOfCurrentRecord)) {
                            flag = true;
                            break;
                        } else {
                            clsOfPrevoisRecord = cls;
                        }
                    }
                    if (!flag) {
                        mr = new MatchableTableRow(r, tblIdx);
                    }
                }

            }

            mr.setPropertyUriToColumnIndex(propertyUriToIndex);
            this.knowledgeIndex.addRecord(mr);
        }
        this.knowledgeIndex.getSizePerTable().put(tblIdx, table.getSize());
        this.knowledgeIndex.addClassIndex(tblIdx, this.computeClassnameFromFileName(table.getPath()));

    }

    void cleanUp(Table table) {
        // remove object properties and keep only "_label" columns (otherwise we will have duplicate property URLs) LodTableColumn[] cols = table.getColumns().toArray(new LodTableColumn[table.getSchema().getSize()]);
        final LodTableColumn[] cols = table.getColumns().toArray(new LodTableColumn[table.getSchema().getSize()]);
        final List<Integer> removedColumns = new LinkedList<>();
        for (final LodTableColumn tc : cols) {
            if (tc.isReferenceLabel()) {
                final Iterator<TableColumn> it = table.getSchema().getRecords().iterator();

                while (it.hasNext()) {
                    final LodTableColumn ltc = (LodTableColumn) it.next();

                    if (!ltc.isReferenceLabel() && ltc.getUri().equals(tc.getUri())) {
                        it.remove();
                        removedColumns.add(ltc.getColumnIndex());
                    }
                }
            }
        }
        // re-create value arrays
        for (final TableRow r : table.getRows()) {
            final Object[] values = new Object[table.getSchema().getSize()];

            int newIndex = 0;
            for (int i = 0; i < r.getValueArray().length; i++) {
                if (!removedColumns.contains(i)) {
                    values[newIndex] = r.getValueArray()[i];
                    newIndex++;
                }
            }

            r.set(values);
        }
    }

    /**
     * @param table Table to match
     * @return True if it's a complex schema and the first columns is the rdf label column
     */
    boolean hasLabelAsKey(Table table) {
        return 1 < table.getSchema().getSize() && "rdf-schema#label".equals(table.getSchema().get(1).getHeader());
    }

    String computeClassnameFromFileName(final String filename) {
        return filename.replace(".csv", "").replace(".gz", "");
    }

    public void serialize() {
        KnowledgeBase.logger.info("Serializing Knowledge Base");

        try (final Output output = new Output(new FileOutputStream(KnowledgeBase.KB_PATH))) {
            final Kryo kryo = KryoFactory.createKryoInstance();
            kryo.writeObject(output, this);
            KnowledgeBase.logger.info("Serialized Knowledge Base");
        } catch (final FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public KnowledgeIndex getKnowledgeIndex() {
        return this.knowledgeIndex;
    }

}
