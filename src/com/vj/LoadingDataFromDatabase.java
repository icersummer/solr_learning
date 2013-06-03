package com.vj;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

public class LoadingDataFromDatabase {
	private static int fetchSize = 1000;
	private static String url = "http://localhost:8983/solr";
	private static HttpSolrServer solrCore;
	
	public LoadingDataFromDatabase(){
		solrCore = new HttpSolrServer(url);
	}
	
	/**
	 * Takes an SQL ResultSet and adds the documents to Solr. 
	 * Does it in batches of fetchSize.
	 * 
	 * @param rs
	 * @return The number of documents added to Solr.
	 * @throws SQLException
	 * @throws IOException 
	 * @throws SolrServerException 
	 */
	public long addResultSet(ResultSet rs) throws SQLException, SolrServerException, IOException {
		long count = 0;
		int innerCount = 0;
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		ResultSetMetaData rsm = rs.getMetaData();
		int numColumns = rsm.getColumnCount();
		String[] colNames = new String[numColumns + 1];
		
		// JDBC numbers the columns starting at 1
		for (int i=1; i < (numColumns + 1); i++){
			colNames[i] = rsm.getColumnName(i);
			// can skip some fields if don't want to handle
		}
		
		while (rs.next()){
			count ++;
			innerCount ++;
			
			SolrInputDocument doc = new SolrInputDocument();
			
			for (int j = 1; j < (numColumns + 1) ; j++){
				if(colNames[j] != null){
					Object f;
					switch (rsm.getColumnType(j)){
						case Types.BIGINT:
							f = rs.getLong(j);
							break;
						case Types.INTEGER:
							f = rs.getInt(j);
							break;
						case Types.DATE:
							f = rs.getDate(j);
							break;
						case Types.FLOAT:
							f = rs.getFloat(j);
							break;
						case Types.DOUBLE:
							f = rs.getDouble(j);
							break;
						case Types.TIME:
							f = rs.getDate(j);
							break;
						case Types.BOOLEAN:
							f = rs.getBoolean(j);
							break;
						default:
							f = rs.getString(j);
							break;
					}
					doc.addField(colNames[j], f);
				}
			}
			
			docs.add(doc);
			
			// index the documents every FetchSize
			if(innerCount == 1000){
				solrCore.add(docs);
				docs.clear();
				innerCount = 0;
			}
		}
		
		if(innerCount > 0){
			solrCore.add(docs);
		}
		
		solrCore.commit();
		
		return count;
	}
	
}
