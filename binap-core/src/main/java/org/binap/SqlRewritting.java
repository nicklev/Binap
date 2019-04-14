package org.binap;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlRewritting {

	private static String[] aggregateFunctions = {"sum", "count"};
	private String query;
	private String rewrittenQuery;
	private String[] splittedQuery;
	static String name = "root";
	static String pass = "";
	static String url = "jdbc:mysql://localhost:3306/metadata?autoReconnect=true&useSSL=false";
	private Connection con;
	
	
	public SqlRewritting(String query) {
		this.query = query;
		try {
			DatabaseConnection dc = new DatabaseConnection(url, name, pass);
			con = dc.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		};
		splittedQuery = query.split(" ");
		
	}
	
	boolean IsQueryAqpValid() {
		int indexOfFrom = 0;
		int indexOfTable = 0;
		PreparedStatement preparedStatement;
		ResultSet rs;
		String leaderColumn = null;
		
		// Aggregation is the second word in a SQL query
		int indexOfLeaderColumn = 1;
		Pattern pattern = Pattern.compile("\\((.*)\\)");
		Matcher m = pattern.matcher(splittedQuery[indexOfLeaderColumn]);
		while(m.find()) {
		    leaderColumn = m.group(1).toString();
		}
		
		for (String str : splittedQuery) {
			if (str.equalsIgnoreCase("FROM"))
			{
				break;
			}
			indexOfFrom++;
		}
		
		indexOfTable = indexOfFrom + 1;
		
		try {
			preparedStatement = con.prepareStatement("SELECT table_name FROM metadata WHERE table_name = ? AND leader_column = ?");
			preparedStatement.setString(1, splittedQuery[indexOfTable]);
			preparedStatement.setString(2, leaderColumn);
			rs = preparedStatement.executeQuery();
			
			if(!rs.isBeforeFirst()) {
				System.out.println("No table with name '" + splittedQuery[indexOfTable] + "' was found");
			}
			else {return true;}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		
		return false;
	}
	
}
