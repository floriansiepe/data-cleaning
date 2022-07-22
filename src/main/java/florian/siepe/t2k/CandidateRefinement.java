/**
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package florian.siepe.t2k;

import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import florian.siepe.control.io.KnowledgeBase;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.MatchableTableRow;
import florian.siepe.entity.kb.WebTables;

import java.util.Map;
import java.util.Set;

/**
 *
 * Component for candidate refinement.
 *
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CandidateRefinement {

    private MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine;
    private IIndex index;
    private String indexLocation;
    private WebTables web;
    private KnowledgeBase kb;
    private SurfaceForms surfaceForms;
    private Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> keyCorrespondences;
    private Map<Integer, Set<String>> classesPerTable;

    private int numCandidates = 100;

    private int maxEditDistance = 1;

    private double similarityThreshold = 0.7;

    public CandidateRefinement(MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine, IIndex index, String indexLocation, WebTables web, KnowledgeBase kb, SurfaceForms surfaceForms, Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> keyCorrespondences, Map<Integer, Set<String>> classesPerTable) {
        this.matchingEngine = matchingEngine;
        this.index = index;
        this.indexLocation = indexLocation;
        this.web = web;
        this.kb = kb;
        this.surfaceForms = surfaceForms;
        this.classesPerTable = classesPerTable;
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
        // set the classes to consider
        candidateGeneratingBlocker.setClassesPerTable(classesPerTable);
        candidateGeneratingBlocker.setNumCandidates(numCandidates);
        candidateGeneratingBlocker.setMaxEditDistance(maxEditDistance);

        // run candidate refinement
        return matchingEngine.runIdentityResolution(web.getRecords(), kb.getKnowledgeIndex().getRecords(), keyCorrespondences, candRule, candidateGeneratingBlocker);

    }

}
