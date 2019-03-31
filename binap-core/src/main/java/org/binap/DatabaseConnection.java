package org.binap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConnection {

	private String name = null;
	private String pass = null;
	private String url = null;
	private Connection con = null;
	
	public DatabaseConnection(String url, String name, String pass) throws SQLException {
		this.name = name;
		this.pass = pass;
		this.url = url;
		this.con = DriverManager.getConnection(url, name, pass); 
	}
	
	public Connection getConnection() {
		return con;
	}
}
