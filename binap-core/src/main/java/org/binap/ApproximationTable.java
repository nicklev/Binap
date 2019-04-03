package org.binap;

import java.sql.*;

public class ApproximationTable {
	
	static private float SAMPLING_PERCENTAGE = 0.1f;
	private String approxTableName;
	private String originalTableName;
	private String leaderColumn;
	private String samplingMethod;
	private Connection con;
	private int numberOfSamples;
	private double binInterval;
	
	//TODO: check if table name already exists
	public ApproximationTable(String approxTableName, String originalTableName,	String leaderColumn, String samplingMethod, Connection con) {
		this.approxTableName = approxTableName;
		this.originalTableName = originalTableName;
		this.leaderColumn = leaderColumn;
		this.samplingMethod = samplingMethod;
		this.con = con;
	}
	
	private boolean MetadataCreation() {
		PreparedStatement preparedStatement;
		
		try {
			preparedStatement = con.prepareStatement("CREATE DATABASE IF NOT EXISTS metadata");
			preparedStatement.execute();
			
			preparedStatement = con.prepareStatement("CREATE TABLE IF NOT EXISTS metadata (id int NOT NULL AUTO_INCREMENT, table_name VARVHAR(255), sampling_method VARCHAR(255), numberOfSamples int, sampling_percentage float);");
			preparedStatement.execute();
			
			preparedStatement = con.prepareStatement("INSERT INTO metadata (table_name, sampling_method, numberOfSamples, sampling_percentage) VALUES (?,?,?,?);");
			preparedStatement.setString(1, approxTableName);
			preparedStatement.setString(2, samplingMethod);
			preparedStatement.setInt(3, numberOfSamples);
			preparedStatement.setFloat(4, SAMPLING_PERCENTAGE);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	//Sampling original table and creating a new one
	//Sampling methods: Random
	boolean OriginalTableSampling() {
		switch (samplingMethod) { 
			case "Random":
				try {
					PreparedStatement preparedStatement = con.prepareStatement("SELECT COUNT(*) FROM " + originalTableName + ";");
					ResultSet rs = preparedStatement.executeQuery(); 
					rs.next();
					numberOfSamples = (int) (rs.getInt(1) * SAMPLING_PERCENTAGE); 
					
					preparedStatement = con.prepareStatement("CREATE TABLE " + approxTableName + " LIKE " + originalTableName + ";");
					preparedStatement.execute();
					
					preparedStatement = con.prepareStatement("INSERT " + approxTableName + " SELECT * FROM " + originalTableName + " ORDER BY RAND() LIMIT " + numberOfSamples + ";");
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

	boolean BinCreation() {
		PreparedStatement preparedStatement;
		ResultSet rs;
		int numberOfBins;
		double minValue, maxValue;
		
		try {
			preparedStatement = con.prepareStatement("ALTER TABLE " + approxTableName + " ADD bin INT DEFAULT -1;");
			preparedStatement.execute();
			
			numberOfBins = (int) (numberOfSamples / Math.log(numberOfSamples) + 1);
			
			preparedStatement = con.prepareStatement("SELECT MIN("+ leaderColumn +"), MAX("+ leaderColumn +"), FROM "+ approxTableName +";");
			rs = preparedStatement.executeQuery();
			rs.next();
			minValue = rs.getDouble(1);
			maxValue = rs.getDouble(2);
			
			binInterval = (maxValue - minValue)/numberOfBins;		
			AssignRowsInBins();
			MetadataCreation();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	private boolean AssignRowsInBins() {
		PreparedStatement preparedStatement;
		
		try {
			preparedStatement = con.prepareStatement("UPDATE "+ approxTableName +" SET bin = "+ leaderColumn + " DIV " + binInterval +";");
			preparedStatement.execute();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	private void CalculateStatistics() {
		
	}
}
