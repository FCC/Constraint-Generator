package com.computechinc.tvmodel.containers;

import java.util.*;

import com.computechinc.tvmodel.ConstrsGenrDriver;

public final class Profile {
	//========================================================================//
	// Class level
	//========================================================================//
	public static class BooleanParamName {
		//
		private static ArrayList<String> booleanParamGlossary = new ArrayList<String>();
		static {
			// Initialize the Glossary here
		}
	}
	//
	public static class DoubleParamName {
		public static String nationwide_acceptable_interference_pct = "nationwide_acceptable_interference_pct";
		//
		private static ArrayList<String> doubleParamGlossary = new ArrayList<String>();
		static {
			// Initialize the Glossary here
			doubleParamGlossary.add(nationwide_acceptable_interference_pct);
		}
	}
	//
	public static class IntegerParamName {
		public static String lowest_channel_num = "lowest_channel_num";
		public static String highest_channel_num = "highest_channel_num";
		//
		private static ArrayList<String> integerParamGlossary = new ArrayList<String>();
		static {
			// Initialize the Glossary here
			integerParamGlossary.add(lowest_channel_num);
			integerParamGlossary.add(highest_channel_num);
		}
	}
	//
	public static class LongParamName {
		//
		private static ArrayList<String> longParamGlossary = new ArrayList<String>();
		static {
			// Initialize the Glossary here
		}
	}
	//
	public static class StringParamName {
		public static String model_class_name = "model_class_name";
		public static String model_class_constructor_arguments_types = "model_class_constructor_arguments_types";
		public static String model_class_constructor_arguments_values = "model_class_constructor_arguments_values";
		//
		public static String database_name = "database_name";
		public static String database_user_name = "database_user_name";
		public static String database_password = "database_password";
		public static String database_ip_address = "database_ip_address";
		public static String database_port = "database_port";
		//
		public static String tv_stations_tablename = "tv_stations_tablename";// MUTABLE
		public static String lm_station_tablename = "lm_station_tablename";// MUTABLE
		public static String lmw_station_tablename = "lmw_station_tablename";// MUTABLE
		public static String lm_lmw_interference_tablename = "lm_lmw_interference_tablename";// MUTABLE
		public static String mx_interference_tablename = "mx_interference_tablename";// MUTABLE
		public static String tvstudy_interference_tablename = "tvstudy_interference_tablename";// MUTABLE
		//
		private static ArrayList<String> stringParamGlossary = new ArrayList<String>();
		static {
			// Initialize the Glossary here
			stringParamGlossary.add(model_class_name);
			stringParamGlossary.add(model_class_constructor_arguments_types);
			stringParamGlossary.add(model_class_constructor_arguments_values);
			//
			stringParamGlossary.add(database_name);
			stringParamGlossary.add(database_user_name);
			stringParamGlossary.add(database_password);
			stringParamGlossary.add(database_ip_address);
			stringParamGlossary.add(database_port);
			//
			stringParamGlossary.add(tv_stations_tablename);
			stringParamGlossary.add(lm_station_tablename);
			stringParamGlossary.add(lmw_station_tablename);
			stringParamGlossary.add(lm_lmw_interference_tablename);
			stringParamGlossary.add(mx_interference_tablename);
			stringParamGlossary.add(tvstudy_interference_tablename);
		}
	}
	//
	public static class StringParamValue {
		public static String None = "None";
		public static String All = "All";
		public static String VHF = "VHF";
		public static String UHF = "UHF";
		public static String LowerVHF = "LowerVHF";
		public static String UpperVHF = "UpperVHF";
	}

	//
	//========================================================================//
	// Instance level of Profile class
	//========================================================================//
	private String name;
	private HashMap<String,Double> key2DoubleValue = new HashMap<String,Double>();
	private HashMap<String,Integer> key2IntegerValue = new HashMap<String,Integer>();
	private HashMap<String,Long> key2LongValue = new HashMap<String,Long>();
	private HashMap<String,String> key2StringValue = new HashMap<String,String>(); 
	private HashMap<String,Boolean> key2BooleanValue = new HashMap<String,Boolean>();
	private HashSet<Long> nationwideProtectedChannelNums = new HashSet<Long>();
	private HashMap<String,HashSet<Long>> stateAbbr2ProtectedChannelNums = new HashMap<String,HashSet<Long>>();
	private HashSet<String> excludedStates = new HashSet<String>();
	//
	// constructors
	public Profile(String name) throws Exception {
		this.name = name;
		this.init();
	}
	//
	// methods
	
	public void addExcludedState(String stateAbbr) {
		if (excludedStates.contains(stateAbbr) == false) {
			excludedStates.add(stateAbbr);
		}
	}
	public void addProtectedChannel(long chNum) {
		nationwideProtectedChannelNums.add(chNum);
		Channel.forbiddenChannelNumbers.add(chNum);
	}
	public void addProtectedChannel(String stateAbbr, long chNum) {
		HashSet<Long> protChannelNums = stateAbbr2ProtectedChannelNums.get(stateAbbr);
		if (protChannelNums == null) {
			protChannelNums = new HashSet<Long>();
			stateAbbr2ProtectedChannelNums.put(stateAbbr, protChannelNums);
		}
		protChannelNums.add(chNum);
	}
	public Boolean getBooleanParam(String paramName) throws Exception {
		if (BooleanParamName.booleanParamGlossary.contains(paramName) == false) {
			ConstrsGenrDriver.displayErr("Error: Illegal boolean parameter name [" + paramName + "].");
			throw new Exception("Error: Illegal boolean parameter name [" + paramName + "].");
		}
		return key2BooleanValue.get(paramName); 
	}
	public Double getDoubleParam(String paramName) throws Exception { 
		if (DoubleParamName.doubleParamGlossary.contains(paramName) == false) {
			ConstrsGenrDriver.displayErr("Error: Illegal double paramater name [" + paramName + "].");
			throw new Exception("Error: Illegal double paramater name [" + paramName + "].");
		}
		return key2DoubleValue.get(paramName); 
	}
	public Integer getIntegerParam(String paramName) throws Exception {
		if (IntegerParamName.integerParamGlossary.contains(paramName) == false) {
			ConstrsGenrDriver.displayErr("Error: Illegal integer paramater name [" + paramName + "].");
			throw new Exception("Error: Illegal integer paramater name [" + paramName + "].");
		}
		return key2IntegerValue.get(paramName); 
	}
	public Long getLongParam(String paramName) throws Exception {
		if (LongParamName.longParamGlossary.contains(paramName) == false) {
			ConstrsGenrDriver.displayErr("Error: Illegal long paramater name [" + paramName + "].");
			throw new Exception("Error: Illegal long paramater name [" + paramName + "].");
		}
		return key2LongValue.get(paramName); 
	}
	public String getName() { return this.name; }
	public HashSet<Long> getProtectedChannelNums() { return new HashSet<Long>(this.nationwideProtectedChannelNums); }
	public HashSet<Long> getProtectedChannelNums(String stateAbbr) {
		HashSet<Long> protectedChannelNums = this.stateAbbr2ProtectedChannelNums.get(stateAbbr);
		if (protectedChannelNums == null) {
			return null;
		}
		else {
			return new HashSet<Long>(this.stateAbbr2ProtectedChannelNums.get(stateAbbr)); 
		}
	}
	public String getStringParam(String paramName) throws Exception {
		if (StringParamName.stringParamGlossary.contains(paramName) == false) {
			ConstrsGenrDriver.displayErr("Error: Illegal string parameter name [" + paramName + "].");
			throw new Exception("Error: Illegal string parameter name [" + paramName + "].");
		}
		return key2StringValue.get(paramName); 
	}
	private void init() throws Exception {
		// Initialize Boolean parameters, default value is false
		for (String paramName : BooleanParamName.booleanParamGlossary) {
			this.key2BooleanValue.put(paramName, false);
		}
		// Initialize Integer type parameters
		this.key2IntegerValue.put(IntegerParamName.lowest_channel_num, 2);
		this.key2IntegerValue.put(IntegerParamName.highest_channel_num, 51);
		// Initialize Double type parameters
		this.key2DoubleValue.put(DoubleParamName.nationwide_acceptable_interference_pct, 0.5);
		// Initialize Long type parameters
		// Initialize String type parameters, default value is null
	}
	public boolean isExcluded(String stateAbbr) {
		if (this.excludedStates.contains(stateAbbr)) {
			return true;
		}
		else {
			return false;
		}
	}
	public void setParam(String paramName,String value) throws Exception {
		if (DoubleParamName.doubleParamGlossary.contains(paramName)) {
			key2DoubleValue.put(paramName,Double.valueOf(value)); 
		}
		else if (IntegerParamName.integerParamGlossary.contains(paramName)) {
			key2IntegerValue.put(paramName,Integer.valueOf(value)); 
		}
		else if (LongParamName.longParamGlossary.contains(paramName)) {
			key2LongValue.put(paramName,Long.valueOf(value)); 
		}
		else if (StringParamName.stringParamGlossary.contains(paramName)) {
			key2StringValue.put(paramName,value);
		}
		else if (BooleanParamName.booleanParamGlossary.contains(paramName)) {
			key2BooleanValue.put(paramName,Boolean.valueOf(value));
		}
		else {
			ConstrsGenrDriver.displayErr("Error: Unknown paramater name [" + paramName + "].");
			throw new Exception("Error: Unknown paramater name [" + paramName + "].");
		}
	}
	public String toString() { 
		return "Profile [" + this.name + "]"; 
	}
}

