package com.computechinc.tvmodel;

import java.io.*;
import java.lang.reflect.*;
import java.text.*;
import java.util.*;
import java.sql.*;

import com.computechinc.tvmodel.containers.*;
import com.computechinc.tvmodel.models.*;

public class ConstrsGenrDriver {
	//========================================================================//
	// Class level
	//========================================================================//
	private static PrintWriter logFile = null;
	private static PrintWriter errFile = null;
	private static Timestamp nCurrDatetime =  new Timestamp(System.currentTimeMillis());
	private static DateFormat nDf = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.FULL, Locale.US);

	public static void displayErr(String str) {
		nCurrDatetime.setTime(System.currentTimeMillis());
		System.err.println("[" + nDf.format(nCurrDatetime) + "]: " + str);
		if (logFile != null) {
			logFile.println("[" + nDf.format(nCurrDatetime) + "]: " + str);
			logFile.flush();
		}
		//
		if (errFile != null) {
			errFile.println("[" + nDf.format(nCurrDatetime) + "]: " + str);
			errFile.flush();
		}
	}
	public static void displayLog() {
		nCurrDatetime.setTime(System.currentTimeMillis());
		System.out.println("[" + nDf.format(nCurrDatetime) + "]: ");
		if (logFile != null) {
			logFile.println("[" + nDf.format(nCurrDatetime) + "]: ");
			logFile.flush();
		}
	}
	public static void displayLog(String str) {
		nCurrDatetime.setTime(System.currentTimeMillis());
		System.out.println("[" + nDf.format(nCurrDatetime) + "]: " + str);
		if (logFile != null) {
			logFile.println("[" + nDf.format(nCurrDatetime) + "]: " + str);
			logFile.flush();
		}
	}
	private static AbstractConstrsGenr instantiateOptModel(Profile profile) throws Exception {
		AbstractConstrsGenr model = null;
		String modelClassName = "com.computechinc.tvmodel.models." + profile.getStringParam(Profile.StringParamName.model_class_name);
		String argTypes = profile.getStringParam(Profile.StringParamName.model_class_constructor_arguments_types);
		String argValues = profile.getStringParam(Profile.StringParamName.model_class_constructor_arguments_values);
		//
		Object[] args = parseArguments(argTypes,argValues);
		Object[] arguments = new Object[args.length + 1];
		arguments[0] = profile;
		for (int i = 1; i < arguments.length; ++i) {
			arguments[i] = args[i-1];
		}
		//
		try {
			Class<?> modelClass = Class.forName(modelClassName);
			Constructor<?>[] constructors = modelClass.getConstructors();
			for (int i = 0; i < constructors.length; ++i) {
				Class<?>[] argumentClasses = constructors[i].getParameterTypes();
				if (argumentClasses.length == arguments.length) {
					boolean foundTheCorrectConstructor = true;
					for (int j = 0; j < arguments.length; ++j) {
						if (argumentClasses[j].isInstance(arguments[j]) == false) {
							foundTheCorrectConstructor = false;
							break;
						}
					}
					//
					if (foundTheCorrectConstructor) {
						model = (AbstractConstrsGenr) constructors[i].newInstance(arguments);
						break;
					}
				}
			}
			//
			return model;
		}
		catch (ClassNotFoundException cEx) {
			ConstrsGenrDriver.displayErr("Error: The Constraint Generator Model class [" + modelClassName + "] is missing.");
			throw cEx;
		}
		catch (Exception ex) {
			throw ex;
		}
	}
	public static void main(String[] args) {
		try {
			String profileName;
			if (args.length == 1) {
				profileName = args[0].trim();
			}
			else {
				System.out.println("Error: Profile name should be the only argument.");
				throw new Exception();
			}
			// Making sure the output folder exists
			File outputFolder = new File("./output");
			if (outputFolder.exists() == false) {
				outputFolder.mkdir();
			}
			// Making sure the log folder exists
			File logFolder = new File("./log");
			if (logFolder.exists() == false) {
				logFolder.mkdir();
			}
			logFile = new PrintWriter(new FileWriter(new File("./log/RunLog_" + profileName + ".log"), false), true);
			errFile = new PrintWriter(new FileWriter(new File("./log/ErrorLog_" + profileName + ".log"), false), true);
			// Read profile file and parse the parameters in it
			ConstrsGenrDriver.displayLog();
			ConstrsGenrDriver.displayLog("Constraint Generator is reading profile file [" + profileName + "].");
			Profile profile = readParameters(profileName/*,profileID*/);
			// Instantiate the Optimization Model class
			ConstrsGenrDriver.displayLog();
			ConstrsGenrDriver.displayLog("Constraint Generator is instantiating the chosen Constraint Generator Model class [" + profile.getStringParam(Profile.StringParamName.model_class_name) + "].");
			AbstractConstrsGenr optModel = instantiateOptModel(profile);
			// Generate Constraints
			optModel.genrConstrs();
			//
			ConstrsGenrDriver.displayLog();
			ConstrsGenrDriver.displayLog("The Constraint Generator is terminated.");
		}
		catch (ClassNotFoundException cEx) {
			ConstrsGenrDriver.displayErr(cEx.getMessage());
			ConstrsGenrDriver.displayErr("The Constraint Generator is terminated with Errors, see log and error file in ./log folder.");
			cEx.printStackTrace(ConstrsGenrDriver.errFile);
		}
		catch (Exception ex) {
			ConstrsGenrDriver.displayErr(ex.getMessage());
			ConstrsGenrDriver.displayErr("The Constraint Generator is terminated with Errors, see log and error file in ./log folder.");
			ex.printStackTrace(ConstrsGenrDriver.errFile);
		}
		finally {
			if (logFile != null) {
				logFile.close();
			}
			if (errFile != null) {
				errFile.close();
			}
		}
	}
	private static Object[] parseArguments(String argTypes, String argValues) throws Exception {
		String[] types = argTypes.split(",");
		String[] values = argValues.split(",");
		boolean profileDataIntegrityStatus = true;
		if (types.length != values.length) {
			ConstrsGenrDriver.displayLog("Error: the counts of argument types versus values are not the same.");
			profileDataIntegrityStatus = false;
		}
		Object[] arguments = new Object[values.length];
		for (int i = 0; i < types.length; ++i) {
			String type = types[i];
			String value = values[i];
			if (type.equalsIgnoreCase(Double.class.getSimpleName())) {
				arguments[i] = Double.valueOf(value);
			}
			else if (type.equalsIgnoreCase(Integer.class.getSimpleName())) {
				arguments[i] = Integer.valueOf(value);
			}
			else if (type.equalsIgnoreCase(String.class.getSimpleName())) {
				arguments[i] = value;
			}
			else if (type.equalsIgnoreCase(Boolean.class.getSimpleName())) {
				arguments[i] = Boolean.valueOf(value);
			}
			else if (type.equalsIgnoreCase(Profile.StringParamValue.None)) {
				return new Object[0];
			}
			else {
				ConstrsGenrDriver.displayErr("Error: Found unknown argumen type [" + type + "]");
				profileDataIntegrityStatus = false;
			}
		}
		//
		if (profileDataIntegrityStatus == false) {
			ConstrsGenrDriver.displayErr("Error: Profile data problem, see the above list");
			throw new Exception("Error: Profile data problem, thrown from static method ConstrsGenrDriver.parseArguments()");
		}
		//
		return arguments;
	}
	private static Profile readParameters(String profileName) throws Exception {
		File profileFile = new File("./profiles/" + profileName);
		if (profileFile.isFile() == false) {
			ConstrsGenrDriver.displayErr("Error: The profile file [./profiles/" + profileName + "] does not exist.");
			throw new Exception("Error: Input file not found problem, thrown from static method ConstrsGenrDriver.readParameters()");
		}
		//
		Profile profile = new Profile(profileName);
		//
		BufferedReader in = new BufferedReader(new FileReader(profileFile));
		if (in.ready() == false) {
			ConstrsGenrDriver.displayErr("Error: The profile file [" + profileName + "] is not ready.");
			in.close();
			throw new Exception("Error: Input file not ready problem, thrown from static method ConstrsGenrDriver.readParameters()");
		}
		String line;
		boolean profileDataIntegrityStatus = true;
		while ((line = in.readLine()) != null) {
			line = line.trim();
			ConstrsGenrDriver.displayLog(line);
			if (line.isEmpty() || line.startsWith("//") || line.startsWith("--")) {
				continue;
			}
			// remove all substring after "//"
			line = line.split("//")[0];
			if (line.isEmpty()) {
				continue;
			}
			// remove all substring after "--"
			line = line.split("--")[0];
			if (line.isEmpty()) {
				continue;
			}
			// remove all substring after ";"
			line = line.split(";")[0];
			if (line.isEmpty()) {
				continue;
			}
			//
			String[] leftStrAndValue = line.split(":");
			String[] tagAndParamName = leftStrAndValue[0].split(",");
			if (tagAndParamName[0].trim().equalsIgnoreCase("MODEL")) {
				profile.setParam(tagAndParamName[1].trim(),leftStrAndValue[1].trim());
			}
			else if (tagAndParamName[0].trim().equalsIgnoreCase("DB_TABLE")) {
				profile.setParam(tagAndParamName[1].trim(),leftStrAndValue[1].trim());
			}
			else {
				ConstrsGenrDriver.displayErr("Error: Unknown parameter tag [" + tagAndParamName[0] + "]");
				profileDataIntegrityStatus = false;
			}
		}
		in.close();
		//
		if (profileDataIntegrityStatus == false) {
			ConstrsGenrDriver.displayErr("Error: Unknown parameter tag problem, see the above list");
			throw new Exception("Error: Unknown parameter tag problem, thrown from a static method ConstrsGenrDriver.readParameters()");
		}
		//
		return profile;
	}
}

