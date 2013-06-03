package com.vj.termSearch;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.TermsResponse;

// used for auto-complete
public class TermsSearchTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TermsSearchTest m = new TermsSearchTest();
		m.search();
	}
	
	/**
	 * available search :
	 * 1. Simple:
	 * 		http://localhost:8983/solr/terms?terms.fl=name&terms.sort=index
	 * 2. Specifying Lower Bound:
	 * 		http://localhost:8983/solr/terms?terms.fl=name&terms.lower=a&terms.sort=index
	 * 3. Auto-Complete:
	 * 		http://localhost:8983/solr/terms?terms.fl=name&terms.prefix=at
	 * JSON response : 
	 * 		http://localhost:8983/solr/terms?terms.fl=name&terms.prefix=at&wt=json&omitHeader=true
	 * Case insensitive Auto-Complete:
	 * 		http://localhost:8983/solr/terms?terms.fl=manu_exact&terms.regex=at.*&terms.regex.flag=case_insensitive
	 */
	void search(){
		SolrServer solrServer = new HttpSolrServer("http://localhost:8983/solr");
		
		SolrQuery params = new SolrQuery();
		params.set("q", "*:*");
		params.set("qt", "/terms");
		
		params.set("terms", "true");
		params.set("terms.fl", "introduction", "name", "text");// term field list
		
		params.set("terms.lower", "");
		params.set("terms.lower.incl", "true");
		params.set("terms.mincount", "1");
		params.set("terms.maxcount", "100");
		
		// terms?terms.fl=text&terms.prefix=v
		// these 2 lines can be commented to get more results
//		params.set("terms.prefix", "v");
//		params.set("terms.regex", "v+.*"); // meaning what?
		params.set("terms.regex.flag", "case_insensitive");
		
		params.set("terms.limit", "20");
		params.set("terms.upper", ""); // mean what?
		params.set("terms.upper.incl", "false");
		
		params.set("terms.raw", "true");
		params.set("terms.sort", "count");
		
		// search it 
		QueryResponse response = null;
		try {
			response = solrServer.query(params);
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		
		if(response == null){
			System.out.println("Response is null.");
			return;
		}
		
		// getting result :
		System.out.println("query time cost(ms) : " + response.getQTime());
		TermsResponse termsResponse = response.getTermsResponse();
		if(termsResponse != null){
			Map<String, List<TermsResponse.Term>> termsMap = termsResponse.getTermMap();
			
			Set<Map.Entry<String, List<TermsResponse.Term>>> termsEntries = termsMap.entrySet();
			Iterator<Map.Entry<String, List<TermsResponse.Term>>> itTermsEntries = termsEntries.iterator();
			while(itTermsEntries.hasNext()){
				Map.Entry<String, List<TermsResponse.Term>> termsEntry = itTermsEntries.next();
				System.out.println("Field Name : " + termsEntry.getKey());
				List<TermsResponse.Term> termsList = termsEntry.getValue();
				System.out.println("Term  :  Frequence");
				for(TermsResponse.Term  term : termsList){
					System.out.printf("%s  :  %s %n", term.getTerm(), term.getFrequency());
				}				
			}

			/*
			// another way 
            Set<String> fieldSet = termsMap.keySet();  
            for(String field : fieldSet) {  
                System.out.println("Field Name : " + field);  
                List<TermsResponse.Term> termList = termsResponse.getTerms(field);  
                System.out.println("Term    :  Frequency");  
                for(TermsResponse.Term term : termList) {  
                    System.out.println(term.getTerm() + "   :   " + term.getFrequency());  
                }  
                System.out.println();  
            }  */
		}
		
		solrServer.shutdown();		
	}
}
