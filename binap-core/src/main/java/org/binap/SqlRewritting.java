package org.binap;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//TODO: the column in the Aggregation clause must be the same with the Where clause
public class SqlRewritting {

	private static String[] aggregateFunctions = {"SUM", "COUNT"};
	private List<Pattern> patterns = new ArrayList<>();
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
		
		patterns.add(Pattern.compile("(SUM)"));
		patterns.add(Pattern.compile("(COUNT"));		
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
	
//	String Rewritter() {
//		String rewrittenSql = "";
//		boolean where = false;
//		Pattern pattern;
//		Matcher m;
//		
//		if (IsQueryAqpValid()) {
//			for (String str : splittedQuery) {
//				if (str == "WHERE") {
//					where = true;
//				}
//				else if (where == true && str != "BETWEEN") {
//					rewrittenSql += BinCalculation()
//				}
//				else {
//					rewrittenSql += str + " ";
//				}
//			}
//		}
//		return rewrittenSql;
//	}
//	
//	private int BinCalculation(int {
//		int bin = 0;
//		
//		return bin;
//	}
	
}
