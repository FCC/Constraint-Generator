package com.computechinc.tvmodel.containers;

import java.util.*;

import com.computechinc.tvmodel.ConstrsGenrDriver;

public final class USStation extends Station {
	//========================================================================//
	// Class level
	//========================================================================//
	private static HashMap<Long,USStation> stationId2CAStation = new HashMap<Long,USStation>();
	private static HashMap<Long,USStation> stationId2DCStation = new HashMap<Long,USStation>();
	private static HashMap<Long,USStation> stationId2DDStation = new HashMap<Long,USStation>();
	private static HashMap<Long,USStation> stationId2DRStation = new HashMap<Long,USStation>();
	private static HashMap<Long,USStation> stationId2DTStation = new HashMap<Long,USStation>();
	private static HashMap<Long,USStation> stationId2LDStation = new HashMap<Long,USStation>();
	private static HashMap<Long,USStation> stationId2LMStation = new HashMap<Long,USStation>();
	private static HashMap<Long,USStation> stationId2LMWStation = new HashMap<Long,USStation>();
	private static HashMap<Long,USStation> stationId2TXStation = new HashMap<Long,USStation>();
	//
	public static Station createCAStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		USStation newCAStation = stationId2CAStation.get(facilityID);
		if (newCAStation == null) {
			newCAStation = new USStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			stationId2CAStation.put(facilityID, newCAStation);
		}
		else {
			ConstrsGenrDriver.displayLog("Warning: Found a duplicate station ID [" + facilityID + "].");
			throw new Exception("Warning: Found a duplicate station ID [" + facilityID + "].");
		}
		return newCAStation;
	}
	public static Station createDCStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		USStation newDCStation = stationId2DCStation.get(facilityID);
		if (newDCStation == null) {
			newDCStation = new USStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			stationId2DCStation.put(facilityID, newDCStation);
		}
		else {
			ConstrsGenrDriver.displayLog("Warning: Found a duplicate station ID [" + facilityID + "].");
			throw new Exception("Warning: Found a duplicate station ID [" + facilityID + "].");
		}
		return newDCStation;
	}
	public static Station createDDStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		USStation newDDStation = stationId2DDStation.get(facilityID);
		if (newDDStation == null) {
			newDDStation = new USStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			stationId2DDStation.put(facilityID, newDDStation);
		}
		else {
			ConstrsGenrDriver.displayLog("Warning: Found a duplicate station ID [" + facilityID + "].");
			throw new Exception("Warning: Found a duplicate station ID [" + facilityID + "].");
		}
		return newDDStation;
	}
	public static Station createDRStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		USStation newDRStation = stationId2DRStation.get(facilityID);
		if (newDRStation == null) {
			newDRStation = new USStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			stationId2DRStation.put(facilityID, newDRStation);
		}
		else {
			ConstrsGenrDriver.displayLog("Warning: Found a duplicate station ID [" + facilityID + "].");
			throw new Exception("Warning: Found a duplicate station ID [" + facilityID + "].");
		}
		return newDRStation;
	}
	public static Station createDTStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		USStation newDTStation = stationId2DTStation.get(facilityID);
		if (newDTStation == null) {
			newDTStation = new USStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			stationId2DTStation.put(facilityID, newDTStation);
		}
		else {
			ConstrsGenrDriver.displayLog("Warning: Found a duplicate station ID [" + facilityID + "].");
			throw new Exception("Warning: Found a duplicate station ID [" + facilityID + "].");
		}
		return newDTStation;
	}
	public static Station createLDStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		USStation newLDStation = stationId2LDStation.get(facilityID);
		if (newLDStation == null) {
			newLDStation = new USStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			stationId2LDStation.put(facilityID, newLDStation);
		}
		else {
			ConstrsGenrDriver.displayLog("Warning: Found a duplicate station ID [" + facilityID + "].");
			throw new Exception("Warning: Found a duplicate station ID [" + facilityID + "].");
		}
		return newLDStation;
	}
	public static Station createLMStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		USStation newLMStation = stationId2LMStation.get(facilityID);
		if (newLMStation == null) {
			newLMStation = new USStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			stationId2LMStation.put(facilityID, newLMStation);
		}
		else {
			ConstrsGenrDriver.displayLog("Warning: Found a duplicate station ID [" + facilityID + "].");
			throw new Exception("Warning: Found a duplicate station ID [" + facilityID + "].");
		}
		return newLMStation;
	}
	public static Station createLMWStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		USStation newLMWStation = stationId2LMWStation.get(facilityID);
		if (newLMWStation == null) {
			newLMWStation = new USStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			stationId2LMWStation.put(facilityID, newLMWStation);
		}
		else {
			ConstrsGenrDriver.displayLog("Warning: Found a duplicate station ID [" + facilityID + "].");
			throw new Exception("Warning: Found a duplicate station ID [" + facilityID + "].");
		}
		return newLMWStation;
	}
	public static Station createTXStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		USStation newTXStation = stationId2TXStation.get(facilityID);
		if (newTXStation == null) {
			newTXStation = new USStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			stationId2TXStation.put(facilityID, newTXStation);
		}
		else {
			ConstrsGenrDriver.displayLog("Warning: Found a duplicate station ID [" + facilityID + "].");
			throw new Exception("Warning: Found a duplicate station ID [" + facilityID + "].");
		}
		return newTXStation;
	}
	//
	public static HashMap<Long,Station> getStationId2CAStation() { return new HashMap<Long,Station>(stationId2CAStation); }
	public static HashMap<Long,Station> getStationId2DCStation() { return new HashMap<Long,Station>(stationId2DCStation); }
	public static HashMap<Long,Station> getStationId2DDStation() { return new HashMap<Long,Station>(stationId2DDStation); }
	public static HashMap<Long,Station> getStationId2DRStation() { return new HashMap<Long,Station>(stationId2DRStation); }
	public static HashMap<Long,Station> getStationId2DTStation() { return new HashMap<Long,Station>(stationId2DTStation); }
	public static HashMap<Long,Station> getStationId2LDStation() { return new HashMap<Long,Station>(stationId2LDStation); }
	public static HashMap<Long,Station> getStationId2LMStation() { return new HashMap<Long,Station>(stationId2LMStation); }
	public static HashMap<Long,Station> getStationId2LMWStation() { return new HashMap<Long,Station>(stationId2LMWStation); }
	public static HashMap<Long,Station> getStationId2TXStation() { return new HashMap<Long,Station>(stationId2TXStation); }
	//
	//========================================================================//
	// Instance level
	//========================================================================//
	//
	// constructors
	private USStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		super(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
	}
	//
	@Override
	public String toString() { return "US_" + super.serviceType +  "[" + super.id + "]"; }
}

