package com.vj;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class MySolrServer {

	public static void main(String[] args) throws SolrServerException, IOException {

//		method1();
		
//		addingDataToSolr();
		
		readingDataFromSolr();
		
		highlighting();
		
		queryWithMultipyParams();
	}
	
	/********************************************************************************************
	 * 
	 ********************************************************************************************/
	static void method1(){
		String url = "http://localhost:8983/solr";
		HttpSolrServer server = new HttpSolrServer(url);
		server.setMaxRetries(1);
		server.setConnectionTimeout(5000);// 5s
		// server.setParser(new XMLResponseParser()); // binary parser is used by default , what's it meaning?
		server.setSoTimeout(1000);// socket read timeout
		server.setDefaultMaxConnectionsPerHost(100);
		server.setMaxTotalConnections(100);
		server.setFollowRedirects(false); // what's used for ?
		server.setAllowCompression(true);

		// pass SolrRequest to SolrServer and return a SolrResponse
		
//		org.apache.http.NoHttpResponseException a;
		
	}

	/********************************************************************************************
	 * 
	 ********************************************************************************************/
	static void addingDataToSolr() throws SolrServerException, IOException{
		System.out.println("creating solr server...");
		SolrServer server = new HttpSolrServer("http://localhost:8983/solr");
		
		// 1.
//		server.deleteByQuery("*:*"); // delete everything
//		System.out.println("clear all done.");
		
		// construct a document
		SolrInputDocument doc1 = new SolrInputDocument();
		doc1.addField("id", "id1", 1.0f); // what's boost used for ?
		doc1.addField("name", "doc1", 1.0f);
		doc1.addField("price", 20);
		
		SolrInputDocument doc2 = new SolrInputDocument();
		doc2.addField("id", "id2", 1.0f);
		doc2.addField("name", "doc2", 1.0f);
		doc2.addField("price", 600);
		
		/* 
		 * Why add these 3 fields?
		 * "id", "name", and "price" are already included in Solr installation, we must add our new custom fields in SchemaXml (link: http://wiki.apache.org/solr/SchemaXml). 
		 */
		
		// add docs to Solr
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		docs.add(doc1);
		docs.add(doc2);
		server.add(docs);
		server.commit(); // commit should be called everytime after: delete, add, update.
		
		System.out.println(" 2 docs added.");
		
		
	}
	
	/********************************************************************************************
	 *     Steaming documents for an update
	 *     haven't tested/used this method
	 ********************************************************************************************/
	void streamingDocumentsForUpdate(){
		HttpSolrServer server = new HttpSolrServer("solr_server_url");
		Iterator<SolrInputDocument> iter = new Iterator<SolrInputDocument>(){
			@Override
			public boolean hasNext() {
				boolean result = false;
				// set the result to true false to say if you have more documents
				return result;
			}

			@Override
			public SolrInputDocument next() {
				SolrInputDocument result = null;
				// construct a new document here and set it to result
				return result;
			}

			@Override
			public void remove() {
				// TODO				
			}			
		};
	}
	
	/********************************************************************************************
	 * reading data from Solr
	 ********************************************************************************************/
	static void readingDataFromSolr(){
		System.out.println(" reading Solr...");
		try {
			
			SolrServer server = new HttpSolrServer("http://localhost:8983/solr");
			
			SolrQuery query = new SolrQuery();
			query.setQuery("*:*");
			query.addSortField("price", SolrQuery.ORDER.asc);
			
			QueryResponse rsp = server.query(query);
			SolrDocumentList docs = rsp.getResults();
			
			Iterator<SolrDocument> it = docs.iterator();
			while(it.hasNext()){
				SolrDocument doc = it.next();
				Collection<String> fields = doc.getFieldNames();
				for(String field : fields){
					Object value = doc.getFieldValue(field);
					System.out.printf("field, value = [ %s, %s ] %n", field, value);		
				}
			}
			
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		
	}

	/********************************************************************************************
	 * advanced usage
	 ********************************************************************************************/
	static void advancedUsageOfQuery(){
		try {
			SolrServer server = null;
			SolrQuery solrQuery = new SolrQuery().
							setQuery("ipod").
							setFacet(true).
							setFacetMinCount(1).
							setFacetLimit(8).
							addFacetField("category").
							addFacetField("inStock");
				QueryResponse rsp = server.query(solrQuery);
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
	}
	

	/********************************************************************************************
	 * highlighting
	 ********************************************************************************************/
	static void highlighting(){
		System.out.println("----------- highlighting ------");
		try {
			SolrServer server = new HttpSolrServer("http://localhost:8983/solr");
			
			SolrQuery query = new SolrQuery();
			query.setQuery("solr");
			query
				.setHighlight(true)
				.setHighlightSnippets(1)     // mean what?
				.setHighlightSimplePre("<b>")
				.setHighlightSimplePost("</b>");
			query.setParam("hl.fl", "name"); //what field you want to highlight, ex. [<em>Solr</em>, the Enterprise Search Server]
//			query.setParam("hl.fl", "features");
		
			QueryResponse queryResponse = server.query(query);
			
			Iterator<SolrDocument> iter = queryResponse.getResults().iterator();
			while(iter.hasNext()){
				SolrDocument resultDoc = iter.next();
				
				String content = (String) resultDoc.getFieldValue("name");
				String id = (String) resultDoc.getFieldValue("id");
				
				if(queryResponse.getHighlighting().get(id) != null){
					Map<String, Map<String, List<String>>> highlighting = queryResponse.getHighlighting();
					Map<String, List<String>> idValue = highlighting.get(id);
					List<String> contentValue = idValue.get("name");
					System.out.println(contentValue);
				}
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
	}
	
	static void queryWithMultipyParams(){
		System.out.println("@queryWithMultipyParams...");
		SolrServer solrServer = new HttpSolrServer("http://localhost:8983/solr");
		SolrQuery params = new SolrQuery();
		
		// common parameters for all search
		params.set("q", "*:*");
		params.set("fq", "age:[20 TO 30]", "grade:[70 TO *]"); // filter query
		params.set("fl", "*,score");// field list
		params.set("sort", "grade desc");// default : score desc
		params.set("start", "0");
		params.set("rows", "10");
		params.set("timeAllowed", "30000"); // 30ms
		params.set("omitHeader", "true");
		params.set("cache", "false");
		
		QueryResponse response = null;
		try {
			response = solrServer.query(params);
		} catch (SolrServerException e) {
			e.printStackTrace();
		} finally {
			solrServer.shutdown();
		}
		
		if(response != null){
			System.out.println("time cost: " + response.getQTime());
			System.out.println("result:\n" + response.toString());
		}
		
	}

}


/*
1, Jars needed: http://mvnrepository.com/artifact/org.apache.solr/solr-solrj/4.3.0

2. Race Condition meet again:
Solr Transactions
Solr implements transactions at the server level. This means that every commit, optimize, or rollback applies to all requests since the last commit/optimize/rollback.
The most appropriate way to update solr is with a single process in order to avoid race conditions when using commit and rollback. Also, ideally the application will use batch processing since commit and optimize can be expensive routines.

3. 

*/