package com.computechinc.tvmodel.util;

import java.sql.*;
import java.util.*;

import com.computechinc.tvmodel.ConstrsGenrDriver;

public class Database {
	//========================================================================//
	// Instance level
	//========================================================================//
	private static String mDriver = "org.postgresql.Driver";
	private static String mURL = "jdbc:postgresql://";
	private static String mIP = "127.0.0.1";
	private static String mPort = "5432";
	private static String mUser = "postgres";
	private static String mPassword = "dbe";

	//========================================================================//
	// Class level
	//========================================================================//
	private Connection mConnection;
	//
	// constructor
	public Database(String dbName, String userName, String password, String ip, String port) throws Exception {
		try {
			mIP = ip;
			mPort = port;
			mURL = mURL + mIP + ":" + mPort + "/" + dbName;
			mUser = userName;
			mPassword = password;
			//
			DriverManager.registerDriver((Driver) Class.forName(mDriver).newInstance());
		}
		catch(Exception ex){ 
			System.err.println( "Unable to load the driver class!" );
			System.err.println(ex.getMessage());
			throw ex;
		}
	}
	//
	// methods

	public void closeConnection() throws SQLException {
		if (mConnection != null) {
			try {
				mConnection.close();
			}
			catch (SQLException ex) {
				ConstrsGenrDriver.displayErr("Error: close connection fails");
				ConstrsGenrDriver.displayErr(ex.getMessage());
				throw ex;
			}
			finally {
				mConnection = null;
			}
		}
	}
	public Connection getConnection() throws SQLException {
		if (mConnection == null) { // first call to method obtains connection
			mConnection = DriverManager.getConnection(mURL, mUser, mPassword);
		}
		//
		return mConnection;
	}
	public ArrayList<HashMap<String, Object>> read_LAND_MOBILE(String tableName) throws SQLException {
		ArrayList<HashMap<String, Object>> records = new ArrayList<HashMap<String, Object>>();
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		//
		String sql = "SELECT * FROM ia." + tableName + " ORDER BY facility_id ";
		//
		try {
			con = this.getConnection();
			stmt = con.createStatement();
			rs = stmt.executeQuery(sql);
			//
			while (rs.next()) {
				HashMap<String, Object> record = new HashMap<String, Object>();
				//
				record.put("facility_id", rs.getLong("facility_id"));
				record.put("fac_channel", rs.getLong("fac_channel"));
				//
				records.add(record);
			}
			//
			return records;
		}
		catch (SQLException ex) {
			ConstrsGenrDriver.displayErr(ex.getMessage());
			throw ex;
		}
		finally {
			if (rs != null) {
				rs.close();
			}
			if (stmt != null) {
				stmt.close();
			}
		}
	}
	public ArrayList<HashMap<String, Object>> read_LAND_MOBILE_WAIVERS(String tableName) throws Exception {
		ArrayList<HashMap<String, Object>> records = new ArrayList<HashMap<String, Object>>();
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		//
		String sql = "SELECT * FROM ia." + tableName + " ORDER BY facility_id ";
		//
		try {
			con = this.getConnection();
			stmt = con.createStatement();
			rs = stmt.executeQuery(sql);
			//
			while (rs.next()) {
				HashMap<String, Object> record = new HashMap<String, Object>();
				//
				record.put("facility_id", rs.getLong("facility_id"));
				record.put("fac_channel", rs.getLong("fac_channel"));
				//
				records.add(record);
			}
			//
			return records;
		}
		catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
		finally {
			if (rs != null) {
				rs.close();
			}
			if (stmt != null) {
				stmt.close();
			}
		}
	}
	public ArrayList<HashMap<String, Object>> read_LM_LMW_INTERFERENCE_TABLE(String tableName) throws SQLException {
		ArrayList<HashMap<String, Object>> records = new ArrayList<HashMap<String, Object>>();
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		//
		String sql = "SELECT * FROM ia." + tableName + " ORDER BY facility_id ";
		//
		try {
			con = this.getConnection();
			stmt = con.createStatement();
			rs = stmt.executeQuery(sql);
			//
			while (rs.next()) {
				HashMap<String, Object> record = new HashMap<String, Object>();
				//
				record.put("facility_id", rs.getLong("facility_id"));
				record.put("prot_channel", rs.getLong("prot_channel"));
				record.put("prot_facility_id", rs.getLong("prot_facility_id"));
				record.put("adj_level", rs.getLong("adj_level"));
				//
				records.add(record);
			}
			//
			return records;
		}
		catch (SQLException ex) {
			ConstrsGenrDriver.displayErr(ex.getMessage());
			throw ex;
		}
		finally {
			if (rs != null) {
				rs.close();
			}
			if (stmt != null) {
				stmt.close();
			}
		}
	}
	public ArrayList<HashMap<String, Object>> read_MX_INTERFERENCE_TABLE(String tableName) throws SQLException {
		ArrayList<HashMap<String, Object>> records = new ArrayList<HashMap<String, Object>>();
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		//
		String sql = "SELECT * FROM ia." + tableName + " ORDER BY facility_id ";
		//
		try {
			con = this.getConnection();
			stmt = con.createStatement();
			rs = stmt.executeQuery(sql);
			//
			while (rs.next()) {
				HashMap<String, Object> record = new HashMap<String, Object>();
				//
				record.put("facility_id", rs.getLong("facility_id"));
				record.put("prot_channel", rs.getLong("prot_channel"));
				record.put("prot_facility_id", rs.getLong("prot_facility_id"));
				record.put("adj_level", rs.getLong("adj_level"));
				//
				records.add(record);
			}
			//
			return records;
		}
		catch (SQLException ex) {
			ConstrsGenrDriver.displayErr(ex.getMessage());
			throw ex;
		}
		finally {
			if (rs != null) {
				rs.close();
			}
			if (stmt != null) {
				stmt.close();
			}
		}
	}
	public ArrayList<HashMap<String, Object>> read_STATION_TABLE(String tableName) throws SQLException {
		ArrayList<HashMap<String, Object>> records = new ArrayList<HashMap<String, Object>>();
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		//
		String sql = "SELECT * FROM ia." + tableName + " ORDER BY facilityid ";
		//
		try {
			con = this.getConnection();
			stmt = con.createStatement();
			rs = stmt.executeQuery(sql);
			//
			while (rs.next()) {
				HashMap<String, Object> record = new HashMap<String, Object>();
				//
				record.put("facility_id", rs.getLong("facilityid"));
				record.put("fac_callsign", trim(rs.getString("callsign")));
				record.put("fac_channel", rs.getLong("channel"));
				record.put("fac_service", trim(rs.getString("servicetypekey")));
				record.put("stateabbr", trim(rs.getString("state")));
				record.put("country", trim(rs.getString("countrycode")));
				//
				records.add(record);
			}
			//
			return records;
		}
		catch (SQLException ex) {
			ConstrsGenrDriver.displayErr(ex.getMessage());
			throw ex;
		}
		finally {
			if (rs != null) {
				rs.close();
			}
			if (stmt != null) {
				stmt.close();
			}
		}
	}
	public ArrayList<HashMap<String, Object>> read_TVSOFTWARE_PAIRWISE_RESULT_FINAL_TABLE(String tableName, long lowerChNum, double marginError) throws SQLException {
		ArrayList<HashMap<String, Object>> records = new ArrayList<HashMap<String, Object>>();
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		//
		long upperChNum = lowerChNum + 1;
		String sql = "SELECT * FROM ia." + tableName + " " +
		"WHERE interference_pct > " + marginError + " " +
		"AND   orig_channel in (" + (lowerChNum - 1) + "," + lowerChNum + "," + upperChNum + "," + (upperChNum + 1) + ") " +
		"AND   interferingchannel in (" + (lowerChNum - 1) + "," + lowerChNum + "," + upperChNum + "," + (upperChNum + 1) + ") " +
		"ORDER BY orig_facilityid, orig_channel, interferingchannel, interferingfacilityid ";
		//String sql = "SELECT * FROM ia." + tableName + " " +
//				"WHERE interference_pct > " + marginError + " " +
//				"AND   orig_channel in (" + lowerChNum + "," + upperChNum + ") " +
//				"AND   interferingchannel in (" + (lowerChNum - 1) + "," + lowerChNum + "," + upperChNum + "," + (upperChNum + 1) + ") " +
//				"ORDER BY orig_facilityid, orig_channel, interferingchannel, interferingfacilityid ";
		//
		try {
			con = this.getConnection();
			stmt = con.createStatement();
			rs = stmt.executeQuery(sql);
			//
			while (rs.next()) {
				HashMap<String, Object> record = new HashMap<String, Object>();
				//
				record.put("orig_facilityid", rs.getLong("orig_facilityid"));
				record.put("orig_channel", rs.getLong("orig_channel"));
				record.put("interferingfacilityid", rs.getLong("interferingfacilityid"));
				record.put("interferingchannel", rs.getLong("interferingchannel"));
				record.put("interference_free_pop", rs.getLong("interference_free_pop"));
				record.put("interference_population", rs.getLong("interference_population"));
				record.put("interference_pct", rs.getDouble("interference_pct"));
				//
				records.add(record);
			}
			//
			return records;
		}
		catch (SQLException ex) {
			ConstrsGenrDriver.displayErr(ex.getMessage());
			throw ex;
		}
		finally {
			if (rs != null) {
				rs.close();
			}
			if (stmt != null) {
				stmt.close();
			}
		}
	}
	private String trim(String string) { return (string != null) ? string.trim() : string; }
}

