package florian.siepe.evaluation;

import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import florian.siepe.control.io.KnowledgeIndex;
import florian.siepe.entity.kb.WebTables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Evaluation {
    private static final Logger logger = LoggerFactory.getLogger(Evaluation.class);

    private MatchingGoldStandard classGs;
    private final KnowledgeIndex index;
    private final WebTables webTables;

    public Evaluation(final KnowledgeIndex index, final WebTables webTables) {
        this.index = index;
        this.webTables = webTables;
    }

    public void loadGoldStandard(File goldStandard) throws IOException {
        classGs = new MatchingGoldStandard();
        classGs.loadFromCSVFile(goldStandard);
        classGs.setComplete(true);
    }

    public void evaluate(final HashMap<Integer, Integer> classMapping) {
        var matchCount = 0;
       logger.info("Found the following classes");
        for (final Map.Entry<Integer, Integer> matching : classMapping.entrySet()) {
            final var ontologyTable = index.getTableIds().inverse().get(matching.getValue());
            final var webTable = webTables.getTablesById().get(matching.getKey());
            var matched = classGs.containsPositive(webTable.getPath(), ontologyTable);
            logger.info("{} - {} / {}", ontologyTable, webTable.getPath(), matched);
            if (matched) {
                matchCount++;
            }
        }
        logger.info("Matched {} of {}", matchCount, classMapping.size());
    }
}
