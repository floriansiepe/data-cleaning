package florian.siepe.t2k;

import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import florian.siepe.control.io.KnowledgeBase;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.MatchableTableRow;
import florian.siepe.entity.kb.WebTables;

public class CandidateSelection {

    private MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine;
    private IIndex index;
    private String indexLocation;
    private WebTables web;
    private KnowledgeBase kb;
    private SurfaceForms surfaceForms;
    private Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> keyCorrespondences;

    private int numCandidates = 50;

    private int maxEditDistance = 0;

    private double similarityThreshold = 0.2;

    public CandidateSelection(MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine,  IIndex index, String indexLocation, WebTables web, KnowledgeBase kb, SurfaceForms surfaceForms, Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> keyCorrespondences) {
        this.matchingEngine = matchingEngine;
        this.index = index;
        this.indexLocation = indexLocation;
        this.web = web;
        this.kb = kb;
        this.surfaceForms = surfaceForms;
        this.keyCorrespondences = keyCorrespondences;
    }

    public Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> run() {
        // create the matching rule
        CandidateSelectionRule candRule = new CandidateSelectionRule(similarityThreshold, kb.getKnowledgeIndex().getRdfsLabel().getColumnIndex());
        KeyValueComparatorBasedOnSurfaceForms keyComparator = new KeyValueComparatorBasedOnSurfaceForms(new WebJaccardStringSimilarity(), kb.getKnowledgeIndex().getRdfsLabel().getColumnIndex(), surfaceForms);
        candRule.setComparator(keyComparator);

        // create the blocker
        CandidateGenerationMultiKeyBlocker candidateGeneratingBlocker = null;
        // if we are using a single machine, pass the index
        candidateGeneratingBlocker = new CandidateGenerationMultiKeyBlocker(index);
        // set redirects to blocker
        candidateGeneratingBlocker.setNumCandidates(numCandidates);
        candidateGeneratingBlocker.setMaxEditDistance(maxEditDistance);

        // run candidate selection
        return matchingEngine.runIdentityResolution(web.getRecords(), kb.getKnowledgeIndex().getRecords(), keyCorrespondences, candRule, candidateGeneratingBlocker);
    }

}

