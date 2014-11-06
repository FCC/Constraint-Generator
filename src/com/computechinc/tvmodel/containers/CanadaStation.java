package com.computechinc.tvmodel.containers;

import java.util.HashMap;

import com.computechinc.tvmodel.ConstrsGenrDriver;

public class CanadaStation extends Station {
	//========================================================================//
	// Class level
	//========================================================================//
	private static HashMap<Long,CanadaStation> stationId2DTCanadaStation = new HashMap<Long,CanadaStation>();
	private static HashMap<Long,CanadaStation> stationId2LDCanadaStation = new HashMap<Long,CanadaStation>();
	private static HashMap<Long,CanadaStation> stationId2TACanadaStation = new HashMap<Long,CanadaStation>();
	private static HashMap<Long,CanadaStation> stationId2TVCanadaStation = new HashMap<Long,CanadaStation>();
	private static HashMap<Long,CanadaStation> stationId2TXCanadaStation = new HashMap<Long,CanadaStation>();
	//
	public static Station createDTCanadaStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		CanadaStation newDTCanadaStation = stationId2DTCanadaStation.get(facilityID);
		if (newDTCanadaStation == null) {
			newDTCanadaStation = new CanadaStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			stationId2DTCanadaStation.put(facilityID, newDTCanadaStation);
		}
		else {
			ConstrsGenrDriver.displayLog("Warning: Found a duplicate station ID [" + facilityID + "].");
			throw new Exception("Warning: Found a duplicate station ID [" + facilityID + "].");
		}
		return newDTCanadaStation;
	}
	public static Station createLDCanadaStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		CanadaStation newLDCanadaStation = stationId2LDCanadaStation.get(facilityID);
		if (newLDCanadaStation == null) {
			newLDCanadaStation = new CanadaStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			stationId2LDCanadaStation.put(facilityID, newLDCanadaStation);
		}
		else {
			ConstrsGenrDriver.displayLog("Warning: Found a duplicate station ID [" + facilityID + "].");
			throw new Exception("Warning: Found a duplicate station ID [" + facilityID + "].");
		}
		return newLDCanadaStation;
	}
	public static Station createTACanadaStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		CanadaStation newTACanadaStation = stationId2TACanadaStation.get(facilityID);
		if (newTACanadaStation == null) {
			newTACanadaStation = new CanadaStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			stationId2TACanadaStation.put(facilityID, newTACanadaStation);
		}
		else {
			ConstrsGenrDriver.displayLog("Warning: Found a duplicate station ID [" + facilityID + "].");
			throw new Exception("Warning: Found a duplicate station ID [" + facilityID + "].");
		}
		return newTACanadaStation;
	}
	public static Station createTVCanadaStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		CanadaStation newTVCanadaStation = stationId2TVCanadaStation.get(facilityID);
		if (newTVCanadaStation == null) {
			newTVCanadaStation = new CanadaStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			stationId2TVCanadaStation.put(facilityID, newTVCanadaStation);
		}
		else {
			ConstrsGenrDriver.displayLog("Warning: Found a duplicate station ID [" + facilityID + "].");
			throw new Exception("Warning: Found a duplicate station ID [" + facilityID + "].");
		}
		return newTVCanadaStation;
	}
	public static Station createTXCanadaStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		CanadaStation newTXCanadaStation = stationId2TXCanadaStation.get(facilityID);
		if (newTXCanadaStation == null) {
			newTXCanadaStation = new CanadaStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			stationId2TXCanadaStation.put(facilityID, newTXCanadaStation);
		}
		else {
			ConstrsGenrDriver.displayLog("Warning: Found a duplicate station ID [" + facilityID + "].");
			throw new Exception("Warning: Found a duplicate station ID [" + facilityID + "].");
		}
		return newTXCanadaStation;
	}
	public static Station findDTCanadaStation(Long facilityID) { return stationId2DTCanadaStation.get(facilityID); }
	public static Station findLDCanadaStation(Long facilityID) { return stationId2LDCanadaStation.get(facilityID); }
	public static Station findTACanadaStation(Long facilityID) { return stationId2TACanadaStation.get(facilityID); }
	public static Station findTVCanadaStation(Long facilityID) { return stationId2TVCanadaStation.get(facilityID); }
	public static Station findTXCanadaStation(Long facilityID) { return stationId2TXCanadaStation.get(facilityID); }
	//
	public static HashMap<Long,CanadaStation> getStationId2DTCanadaStation() { return new HashMap<Long,CanadaStation>(stationId2DTCanadaStation); }
	public static HashMap<Long,CanadaStation> getStationId2LDCanadaStation() { return new HashMap<Long,CanadaStation>(stationId2LDCanadaStation); }
	public static HashMap<Long,CanadaStation> getStationId2TACanadaStation() { return new HashMap<Long,CanadaStation>(stationId2TACanadaStation); }
	public static HashMap<Long,CanadaStation> getStationId2TVCanadaStation() { return new HashMap<Long,CanadaStation>(stationId2TVCanadaStation); }
	public static HashMap<Long,CanadaStation> getStationId2TXCanadaStation() { return new HashMap<Long,CanadaStation>(stationId2TXCanadaStation); }
	//
	//========================================================================//
	// Instance level
	//========================================================================//
	//
	// constructors
	public CanadaStation(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateAbbr) throws Exception {
		super(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
	}
	//
	//
	@Override
	public String toString() { return "CA_" + super.serviceType + "[" + super.id + "]"; }
}

