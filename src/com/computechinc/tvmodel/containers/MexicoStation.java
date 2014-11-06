package com.computechinc.tvmodel.containers;

import java.util.HashMap;

import com.computechinc.tvmodel.ConstrsGenrDriver;

public class MexicoStation extends Station {
	//========================================================================//
	// Class level
	//========================================================================//
	private static HashMap<Long,MexicoStation> stationId2DTMexicoStation = new HashMap<Long,MexicoStation>();
	private static HashMap<Long,MexicoStation> stationId2LDMexicoStation = new HashMap<Long,MexicoStation>();
	private static HashMap<Long,MexicoStation> stationId2TAMexicoStation = new HashMap<Long,MexicoStation>();
	private static HashMap<Long,MexicoStation> stationId2TVMexicoStation = new HashMap<Long,MexicoStation>();
	private static HashMap<Long,MexicoStation> stationId2TXMexicoStation = new HashMap<Long,MexicoStation>();
	//
	public static Station createDTMexicoStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		MexicoStation newDTMexicoStation = stationId2DTMexicoStation.get(facilityID);
		if (newDTMexicoStation == null) {
			newDTMexicoStation = new MexicoStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			stationId2DTMexicoStation.put(facilityID, newDTMexicoStation);
		}
		else {
			ConstrsGenrDriver.displayLog("Warning: Found a duplicate station ID [" + facilityID + "].");
			throw new Exception("Warning: Found a duplicate station ID [" + facilityID + "].");
		}
		return newDTMexicoStation;
	}
	public static Station createLDMexicoStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		MexicoStation newLDMexicoStation = stationId2LDMexicoStation.get(facilityID);
		if (newLDMexicoStation == null) {
			newLDMexicoStation = new MexicoStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			stationId2LDMexicoStation.put(facilityID, newLDMexicoStation);
		}
		else {
			ConstrsGenrDriver.displayLog("Warning: Found a duplicate station ID [" + facilityID + "].");
			throw new Exception("Warning: Found a duplicate station ID [" + facilityID + "].");
		}
		return newLDMexicoStation;
	}
	public static Station createTAMexicoStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		MexicoStation newTAMexicoStation = stationId2TAMexicoStation.get(facilityID);
		if (newTAMexicoStation == null) {
			newTAMexicoStation = new MexicoStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			stationId2TAMexicoStation.put(facilityID, newTAMexicoStation);
		}
		else {
			ConstrsGenrDriver.displayLog("Warning: Found a duplicate station ID [" + facilityID + "].");
			throw new Exception("Warning: Found a duplicate station ID [" + facilityID + "].");
		}
		return newTAMexicoStation;
	}
	public static Station createTVMexicoStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		MexicoStation newTVMexicoStation = stationId2TVMexicoStation.get(facilityID);
		if (newTVMexicoStation == null) {
			newTVMexicoStation = new MexicoStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			stationId2TVMexicoStation.put(facilityID, newTVMexicoStation);
		}
		else {
			ConstrsGenrDriver.displayLog("Warning: Found a duplicate station ID [" + facilityID + "].");
			throw new Exception("Warning: Found a duplicate station ID [" + facilityID + "].");
		}
		return newTVMexicoStation;
	}
	public static Station createTXMexicoStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		MexicoStation newTXMexicoStation = stationId2TXMexicoStation.get(facilityID);
		if (newTXMexicoStation == null) {
			newTXMexicoStation = new MexicoStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			stationId2TXMexicoStation.put(facilityID, newTXMexicoStation);
		}
		else {
			ConstrsGenrDriver.displayLog("Warning: Found a duplicate station ID [" + facilityID + "].");
			throw new Exception("Warning: Found a duplicate station ID [" + facilityID + "].");
		}
		return newTXMexicoStation;
	}
	public static Station findDTMexicoStation(Long facilityID) { return stationId2DTMexicoStation.get(facilityID); }
	public static Station findLDMexicoStation(Long facilityID) { return stationId2LDMexicoStation.get(facilityID); }
	public static Station findTAMexicoStation(Long facilityID) { return stationId2TAMexicoStation.get(facilityID); }
	public static Station findTVMexicoStation(Long facilityID) { return stationId2TVMexicoStation.get(facilityID); }
	public static Station findTXMexicoStation(Long facilityID) { return stationId2TXMexicoStation.get(facilityID); }
	//
	public static HashMap<Long,MexicoStation> getStationId2DTMexicoStation() { return new HashMap<Long,MexicoStation>(stationId2DTMexicoStation); }
	public static HashMap<Long,MexicoStation> getStationId2LDMexicoStation() { return new HashMap<Long,MexicoStation>(stationId2LDMexicoStation); }
	public static HashMap<Long,MexicoStation> getStationId2TAMexicoStation() { return new HashMap<Long,MexicoStation>(stationId2TAMexicoStation); }
	public static HashMap<Long,MexicoStation> getStationId2TVMexicoStation() { return new HashMap<Long,MexicoStation>(stationId2TVMexicoStation); }
	public static HashMap<Long,MexicoStation> getStationId2TXMexicoStation() { return new HashMap<Long,MexicoStation>(stationId2TXMexicoStation); }
	
	//========================================================================//
	// Instance level
	//========================================================================//
	public MexicoStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		super(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
	}	
	@Override
	public String toString() { return "MX_" + super.serviceType + "[" + super.id + "]"; }

}
