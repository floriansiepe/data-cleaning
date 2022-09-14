package florian.siepe.matcher;

import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import florian.siepe.entity.kb.MatchableTableColumn;

import java.util.Map;

public interface Matcher {
    Map<Integer, Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>>> runMatching();
}
