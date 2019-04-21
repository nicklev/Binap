package org.binap;

import java.util.Scanner;

public class Main {

	static void print() {
		System.out.println("Insert '1' to create a new approximate table.");
		System.out.println("Insert '2' to insert a query.");
		System.out.println("Insert '3' to exit the program.");
	}
	
	static String printInsert(String message) {
		Scanner userInput = new Scanner(System.in);
		String input;
		System.out.println(message);
		input = userInput.nextLine();
		userInput.close();
		return input;
	}
	
	public static void main(String[] args) {
		Scanner userInput = new Scanner(System.in);
		String input;
		String originalTableName;
		String leaderColumn;
		String samplingMethod;
		String approxTableName;
		
		do {
			print();
			System.out.println("Insert your choice: ");
			input = userInput.nextLine();
			switch(input) {
				case "1":
					approxTableName = printInsert("Insert the name of the new approximate table: ");
					originalTableName = printInsert("Insert the name of the original table: ");
					leaderColumn = printInsert("Insert the leader column name: ");
					//samplingMethod = printInsert("Insert the samplingMethod: ");
					
					break;
				case "2":
					break;
				case "3":
					break;
				default:
					break;
			}
			
			
		
			
		} while(userInput.toString() != "exit");
		
	}

}
