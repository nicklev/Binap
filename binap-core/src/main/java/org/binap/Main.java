package org.binap;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Scanner;

public class Main {

	static void printMenu() {
		System.out.println("Insert '1' to create a new approximate table.");
		System.out.println("Insert '2' to insert a query.");
		System.out.println("Insert '3' to exit the program.");
	}
	
	static String printInsert(String message) {
		Scanner userInput = new Scanner(System.in);
		String input = null;
		System.out.println(message);
		
		input = userInput.nextLine();

		return input;
	}
	
	public static void main(String[] args) {
		Scanner userInput = new Scanner(System.in);
		String input;
		String originalTableName;
		String leaderColumn;
		String samplingMethod;
		String approxTableName;
		String query;
		String name = "root";
		String pass = "";
		String url = "jdbc:mysql://localhost:3306/binap?autoReconnect=true&useSSL=false";
		DatabaseConnection databaseConnection;
		PreparedStatement preparedStatement;
		Connection con = null;
		try {
			databaseConnection = new DatabaseConnection(url, name, pass);
			con = databaseConnection.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		do {
			printMenu();
			System.out.println("Insert your choice: ");
			input = userInput.nextLine();
			switch(input) {
				case "1":
					approxTableName = printInsert("Insert the name of the new approximate table: ");
					originalTableName = printInsert("Insert the name of the original table: ");
					leaderColumn = printInsert("Insert the leader column name: ");
					//samplingMethod = printInsert("Insert the samplingMethod: ");
					samplingMethod = "Random";
					ApproximationTable approximationTable = new ApproximationTable(approxTableName, originalTableName, leaderColumn, samplingMethod, con);
					approximationTable.OriginalTableSampling();
					approximationTable.BinCreation();
					break;
				case "2":
					query = printInsert("Insert a SQL query: ");
					SqlRewritting sqlRewritting = new SqlRewritting(query);
					String rewrittenQuery = sqlRewritting.Rewritter();
					try {
						preparedStatement = con.prepareStatement(rewrittenQuery);
						preparedStatement.execute();
					} catch (SQLException e) {
						e.printStackTrace();
					}
					break;
				case "3":
					System.out.println("Exiting the program...");
					break;
				default:
					break;
			}
		} while(!input.equals("3"));
		
	}

}
