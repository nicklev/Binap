package org.binap;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;

class ApproximationTableTest {

	static String name = "root";
	static String pass = "";
	static String url = "jdbc:mysql://localhost:3306/binap?autoReconnect=true&useSSL=false";
		
	@Test
	void OriginalTableNameCreatesNewSampledTable() {
		try {
			DatabaseConnection dc = new DatabaseConnection(url, name, pass);
			ApproximationTable at = new ApproximationTable("test_table", "aisles", "id", "Random", dc.getConnection());
			
			if (at.OriginalTableSampling() == false)
			{
				fail();
				PreparedStatement preparedStatement = dc.getConnection().prepareStatement("DROP TABLE test_table");
				preparedStatement.execute();
			}
				
		} catch (SQLException e) {
			fail(e);
		}
	}

}
