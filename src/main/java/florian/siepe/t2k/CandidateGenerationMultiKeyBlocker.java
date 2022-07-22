/** 
 *
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package florian.siepe.t2k;

import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.AbstractBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.Blocker;
import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.processing.*;
import florian.siepe.entity.kb.MatchableTableColumn;
import florian.siepe.entity.kb.MatchableTableRow;

import java.util.Map;
import java.util.Set;

/**
 * 
 * Blocker used in Candidate Generation and Candidate Refinement that loads candidates from an index.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CandidateGenerationMultiKeyBlocker 
	extends AbstractBlocker<MatchableTableRow,MatchableTableRow, MatchableTableColumn> //<MatchableTableRow, MatchableTableColumn, MatchableTableColumn>
	implements Blocker<MatchableTableRow, MatchableTableColumn, MatchableTableRow, MatchableTableColumn>
{

	public CandidateGenerationMultiKeyBlocker(String indexLocation) {
		lookup.setIndex(indexLocation);
	}
	
	public String getIndexLocation(){
		return lookup.getIndexLocation();
	}
	
	public CandidateGenerationMultiKeyBlocker(IIndex index) {
		lookup.setIndex(index);
	}
	
	public IIndex getIndex(){
		return lookup.getIndex();
	}
	
	public void setNumCandidates(int numCandidates) {
		lookup.setNumDocuments(numCandidates);
	}
	
	public int getNumCandidates(){
		return lookup.getNumDoc();
	}
	
	public void setMaxEditDistance(int maxDistance) {
		lookup.setMaxEditDistance(maxDistance);
	}
	
	public int getMaxEditDistance(){
		return lookup.getDist();
	}
	
	private KeyIndexLookup lookup = new KeyIndexLookup();
	
	/**
	 * @param classesPerTable the classesPerTable to set
	 */
	public void setClassesPerTable(Map<Integer, Set<String>> classesPerTable) {
		lookup.setClassesPerTable(classesPerTable);
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.blocking.MultiKeyBlocker#runBlocking(de.uni_mannheim.informatik.wdi.model.DataSet, de.uni_mannheim.informatik.wdi.model.DataSet, de.uni_mannheim.informatik.wdi.model.ResultSet, de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine)
	 */
	@Override
	public Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> runBlocking(
			DataSet<MatchableTableRow, MatchableTableColumn> dataset1,
			DataSet<MatchableTableRow, MatchableTableColumn> dataset2,
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		
		// dataset1 web tables
		// dataset2 dbpedia
		// schema correspondences contain the keys of the web tables and map them to rdfs:label
		
		// to be able to get the key value of each row, we must first join the rows with the schema correspondences based on the web table id
		
		Function<Integer, MatchableTableRow> webTableRowToTableId = new Function<Integer, MatchableTableRow>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(MatchableTableRow input) {
				return input.getTableId();
			}
		};
		
//		Function<Integer, Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondenceToFirstTableId = new Function<Integer, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Integer execute(Correspondence<MatchableTableColumn, ?> input) {
//				return input.getFirstRecord().getTableId();
//			}
//		};
		
		// this join results in: <web table row, schema correspondence>
		Processable<Pair<MatchableTableRow, Correspondence<MatchableTableColumn, Matchable>>> rowsWithKeyCorrespondences 
		= dataset1.join(schemaCorrespondences, webTableRowToTableId, 
				(Correspondence<MatchableTableColumn, Matchable> input) -> input.getFirstRecord().getTableId());
		
		// now we can generate blocking keys for the web table rows with the schema correspondences and for the dbpedia rows
		// as we can get multiple blocking keys for each row, we first transform the rows to a new dataset of rows with their blocking keys 
		
		RecordMapper<Pair<MatchableTableRow, Correspondence<MatchableTableColumn, Matchable>>, Triple<String, MatchableTableRow, Correspondence<MatchableTableColumn, Matchable>>> webTableRowBlockingFunction = new RecordMapper<Pair<MatchableTableRow,Correspondence<MatchableTableColumn, Matchable>>, Triple<String, MatchableTableRow, Correspondence<MatchableTableColumn, Matchable>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<MatchableTableRow, Correspondence<MatchableTableColumn, Matchable>> record,
					DataIterator<Triple<String, MatchableTableRow, Correspondence<MatchableTableColumn, Matchable>>> resultCollector) {
				// key the row's key value
				Object keyValue = record.getFirst().get(record.getSecond().getFirstRecord().getColumnIndex());
				
				if(keyValue!=null) {
					for(String blockingKey : lookup.searchIndex(keyValue, record.getFirst().getTableId())) {
						resultCollector.next(new Triple<>(blockingKey, record.getFirst(), record.getSecond()));
					}
				}
			}
		};
		
		Processable<Triple<String, MatchableTableRow, Correspondence<MatchableTableColumn, Matchable>>> blockedWebTableRows = rowsWithKeyCorrespondences.map(webTableRowBlockingFunction);
		
		// now generate the blocking keys for the dbpedia rows
		RecordMapper<MatchableTableRow, Pair<String, MatchableTableRow>> dbpediaRowBlockingFunction = new RecordMapper<MatchableTableRow, Pair<String,MatchableTableRow>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(MatchableTableRow record, DataIterator<Pair<String, MatchableTableRow>> resultCollector) {
				
				// simply return the URI
				
				resultCollector.next(new Pair<String, MatchableTableRow>(record.get(0).toString(), record));
				
			}
		};
		
		Processable<Pair<String, MatchableTableRow>> blockedDBpediaRows = dataset2.map(dbpediaRowBlockingFunction);
		
		// then we can join both results via the URI (the String in the pair/triple)
//		Function<String, Triple<String, MatchableTableRow, Correspondence<MatchableTableColumn, MatchableTableRow>>> tripleJoinKeyGenerator = new Function<String, Triple<String, MatchableTableRow, Correspondence<MatchableTableColumn, MatchableTableRow>>>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public String execute(Triple<String, MatchableTableRow, Correspondence<MatchableTableColumn, MatchableTableRow>> input) {
//				return input.getFirst();
//			}
//		};
//		Function<String, Pair<String, MatchableTableRow>> pairJoinKeyGenerator = new Function<String, Pair<String,MatchableTableRow>>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public String execute(Pair<String, MatchableTableRow> input) {
//				return input.getFirst();
//			}
//		};
	
		Processable<Pair<Triple<String, MatchableTableRow, Correspondence<MatchableTableColumn, Matchable>>, Pair<String, MatchableTableRow>>> blocks = 
				blockedWebTableRows.join(blockedDBpediaRows, 
						(Triple<String, MatchableTableRow, Correspondence<MatchableTableColumn, Matchable>> input) -> input.getFirst(), 
						(Pair<String, MatchableTableRow> input) -> input.getFirst());
		
		// finally, transform the data into BlockedMatchable
		
		RecordMapper<Pair<Triple<String, MatchableTableRow, Correspondence<MatchableTableColumn, Matchable>>, Pair<String, MatchableTableRow>>, Correspondence<MatchableTableRow, MatchableTableColumn>> blockedMatchableTransformation = new RecordMapper<Pair<Triple<String, MatchableTableRow, Correspondence<MatchableTableColumn, Matchable>>,Pair<String,MatchableTableRow>>, Correspondence<MatchableTableRow,MatchableTableColumn>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<Triple<String, MatchableTableRow, Correspondence<MatchableTableColumn, Matchable>>, Pair<String, MatchableTableRow>> record,
					DataIterator<Correspondence<MatchableTableRow, MatchableTableColumn>> resultCollector) {
			
				Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences = new ProcessableCollection<>();
				correspondences.add(record.getFirst().getThird());
				resultCollector.next(new Correspondence<MatchableTableRow, MatchableTableColumn>(
						record.getFirst().getSecond(), 
						record.getSecond().getSecond(), 
						1.0,
						correspondences));
				
			}
		};

		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> result = blocks.map(blockedMatchableTransformation);
		
		calculatePerformance(dataset1, dataset2, result);
		
		return result;
	}

}
