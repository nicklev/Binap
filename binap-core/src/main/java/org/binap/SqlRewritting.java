package org.binap;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import com.mysql.jdbc.Connection;

public class SqlRewritting {

	private static String[] aggregateFunctions = {"sum", "count"};
	private String query;
	private String rewrittenQuery;
	private String[] splittedQuery;
	
	
	public SqlRewritting(String query, Connenction con) {
		this.query = query;
		splittedQuery = query.split(" ");
		
	}
	
	boolean IsQueryAqpValid() {
		int indexOfFrom = 0;
		
		for (String str : splittedQuery) {
			if (str.equalsIgnoreCase("FROM"))
			{
				break;
			}
			indexOfFrom++;
		}
		
		
		return false;
	}
	
	
	private boolean IsColumnInAqpTable(int indexOfFrom) {
		PreparedStatement preparedStatement;
		
		preparedStatement = 
		
		
		return false;
	}
}
