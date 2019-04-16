package org.binap;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;


class SqlRewrittingTest {
	static String name = "root";
	static String pass = "";
	static String url = "jdbc:mysql://localhost:3306/binap?autoReconnect=true&useSSL=false";
	
	@Test
	void IsQueryAqpValidReturnsTrueIfTableNameAndLeaderColumnExistsInMetadata() {
		String query = "SELECT SUM(id) FROM test_table";
		SqlRewritting sqlRewritting = new SqlRewritting(query);
		
		assertEquals(true, sqlRewritting.IsQueryAqpValid());		
	}

	@Test
	void MatchTestReturnsTrueIfStringsMatches() {
		String query = "SELECT SUM(id) FROM test_aqp";
		String[] splittedQuery = query.split(" ");
		
		Pattern pattern = Pattern.compile("\\((.*)\\)");
		Matcher m = pattern.matcher(splittedQuery[1]);
		String test = null;
		while(m.find()) {
		    test = m.group(1).toString();
		}
				
		assertEquals("id", test);
	}
	
	@Test
	void BinCalculationTestReturnsTrue() {
		int columnId = 5;
		int expected;
		String query = "SELECT SUM(id) FROM test_table WHERE id BETWEEN 5 AND 10";
		SqlRewritting sqlRewritting = new SqlRewritting(query);
		int real = sqlRewritting.BinCalculation(columnId);
	}
}
