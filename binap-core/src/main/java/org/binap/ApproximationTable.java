package org.binap;

import java.sql.*;

public class ApproximationTable {
	
	static private float SAMPLING_PERCENTAGE = 0.1f;
	private String approxTableName;
	private String originalTableName;
	private String leaderColumn;
	private String samplingMethod = null;
	private Connection userDatabaseConnection;
	private int numberOfSamples;
	private double binInterval;
//	private static String name = "root";
//	private static String pass = "";
//	private static String url = "jdbc:mysql://localhost:3306/metadata?autoReconnect=true&useSSL=false";
	
	//TODO: check if table name already exists
	public ApproximationTable(String approxTableName, String originalTableName,	String leaderColumn, String samplingMethod, Connection userDatabaseConnection) {
		this.approxTableName = approxTableName;
		this.originalTableName = originalTableName;
		this.leaderColumn = leaderColumn;
		this.samplingMethod = samplingMethod;
		this.userDatabaseConnection = userDatabaseConnection;
	}
	
	//Creates bin column, calculates total number of bins and
	//calls AssignRowsInBins, MetadataCreation
	boolean BinCreation() {
		PreparedStatement preparedStatement;
		ResultSet rs;
		int numberOfBins;
		double minValue, maxValue;
		
		if (samplingMethod == null) {
			OriginalTableSampling();
		}
		
		try {
			preparedStatement = userDatabaseConnection.prepareStatement("ALTER TABLE " + approxTableName + " ADD bin INT DEFAULT -1;");
			preparedStatement.execute();

			numberOfBins = (int) (numberOfSamples / Math.log(numberOfSamples) + 1);

			preparedStatement = userDatabaseConnection.prepareStatement("SELECT MIN("+ leaderColumn +"), MAX("+ leaderColumn +") FROM "+ approxTableName +";");
			rs = preparedStatement.executeQuery();
			rs.next();
			minValue = rs.getDouble(1);
			maxValue = rs.getDouble(2);

			binInterval = (maxValue - minValue)/numberOfBins;	
			
			System.out.println("Assign rows in bins...");
			if (!AssignRowsInBins()) { System.out.println("Failed to assign rows in bins."); return false;}
			System.out.println("Creating metadata...");
			if (!MetadataCreation()) { System.out.println("The creation of Metadata failed."); return false;}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		
		System.out.println("Approximate table created.");
		return true;
	}
	
	private boolean MetadataCreation() {
		PreparedStatement preparedStatement;
		
		try {
			preparedStatement = userDatabaseConnection.prepareStatement("CREATE DATABASE IF NOT EXISTS metadata");
			preparedStatement.execute();
			
			userDatabaseConnection.setCatalog("metadata");
			
			preparedStatement = userDatabaseConnection.prepareStatement("CREATE TABLE IF NOT EXISTS metadata (id int NOT NULL AUTO_INCREMENT, table_name VARCHAR(255), original_table_name VARCHAR(255), sampling_method VARCHAR(255), number_of_samples int, sampling_percentage float, leader_column VARCHAR(255), PRIMARY KEY (id));");
			preparedStatement.execute();
			
			preparedStatement = userDatabaseConnection.prepareStatement("INSERT INTO metadata (table_name, original_table_name, sampling_method, number_of_samples, sampling_percentage, leader_column) VALUES (?,?,?,?,?,?);");
			preparedStatement.setString(1, approxTableName);
			preparedStatement.setString(2, originalTableName);
			preparedStatement.setString(3, samplingMethod);
			preparedStatement.setInt(4, numberOfSamples);
			preparedStatement.setFloat(5, SAMPLING_PERCENTAGE);
			preparedStatement.setString(6, leaderColumn);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	//Sampling original table and creating a new one
	//Sampling methods: Random
	//TODO: add more sampling methods and create user input if sampling method doesn't exist
	boolean OriginalTableSampling() {
		switch (samplingMethod) { 
			case "Random":
				try {
					PreparedStatement preparedStatement = userDatabaseConnection.prepareStatement("SELECT COUNT(*) FROM " + originalTableName + ";");
					ResultSet rs = preparedStatement.executeQuery(); 
					rs.next();
					numberOfSamples = (int) (rs.getInt(1) * SAMPLING_PERCENTAGE); 
					
					preparedStatement = userDatabaseConnection.prepareStatement("CREATE TABLE " + approxTableName + " LIKE " + originalTableName + ";");
					preparedStatement.execute();
					
					preparedStatement = userDatabaseConnection.prepareStatement("INSERT " + approxTableName + " SELECT * FROM " + originalTableName + " ORDER BY RAND() LIMIT " + numberOfSamples + ";");
					preparedStatement.execute();			
				} catch (SQLException e) {
					e.printStackTrace(); 
					return false;
				}
				break;
			default:
				System.out.println("Sampling method " + samplingMethod + " is not supported.");
		}
		return true;
	}

	private boolean AssignRowsInBins() {
		PreparedStatement preparedStatement;
		
		try {
			preparedStatement = userDatabaseConnection.prepareStatement("UPDATE "+ approxTableName +" SET bin = "+ leaderColumn + " DIV " + binInterval +";");
			preparedStatement.execute();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
}
