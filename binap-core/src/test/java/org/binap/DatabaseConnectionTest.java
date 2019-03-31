package org.binap;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

class DatabaseConnectionTest {		
		@Test
		public void DatabaseConnectionNoExceptionTest() {
				try {
					String name = "root";
					String pass = "";
					String url = "jdbc:mysql://localhost:3306/binap?autoReconnect=true&useSSL=false";
					
					DatabaseConnection dc = new DatabaseConnection(url, name, pass);
				} catch (SQLException e) {
					fail(e.toString());
				}		
		}
}
