package com.computechinc.tvmodel.models;

import com.computechinc.tvmodel.*;
import com.computechinc.tvmodel.containers.*;
import com.computechinc.tvmodel.util.*;

import java.io.*;
import java.util.*;

public abstract class AbstractConstrsGenr {
	//========================================================================//
	// Class level
	//========================================================================//
	public static Database db;

	//========================================================================//
	// Instance level
	//========================================================================//
	protected Profile profile;
	protected LinkedHashMap<Long,Station> stationId2ConsideredStation = new LinkedHashMap<Long,Station>();
	protected LinkedHashMap<Long,Station> stationId2ProtectedStation = new LinkedHashMap<Long,Station>();
	protected ArrayList<Station> consideredUHFStations = new ArrayList<Station>();
	protected ArrayList<Station> consideredLowerVHFStations = new ArrayList<Station>();
	protected ArrayList<Station> consideredUpperVHFStations = new ArrayList<Station>();
	protected HashSet<Long> excludedStationIds = new HashSet<Long>();
	protected Double marginError = new Double(0.0001);
	//
	// constructors
	protected AbstractConstrsGenr(Profile profile) throws Exception {
		this.profile = profile;
		//
		String dbName = profile.getStringParam(Profile.StringParamName.database_name);
		String dbUserName = profile.getStringParam(Profile.StringParamName.database_user_name);
		String dbPassword = profile.getStringParam(Profile.StringParamName.database_password);
		String dbIPAddress = profile.getStringParam(Profile.StringParamName.database_ip_address);
		String dbPort = profile.getStringParam(Profile.StringParamName.database_port);
		AbstractConstrsGenr.db = new Database(dbName, dbUserName, dbPassword, dbIPAddress, dbPort);
		//
		initAbstractLevel();
	}
	//
	// methods

	protected void appendAdjChannelInterferencePairedCSV(ArrayList<Station> consideredStations, Channel lowerChannel, Channel upperChannel) throws Exception {
		PrintWriter outStream = new PrintWriter(new FileWriter(new File("./output/Interference_Paired.csv"), true), true);
		Collections.sort(consideredStations, Comparators.StationByAscFacilityId);
		//
		long domainLB = lowerChannel.getChannelNum();
		long domainUB = upperChannel.getChannelNum();
		String adjBelowTag = "ADJ-1";
		String adjAboveTag = "ADJ+1";
		String lineOut;
		boolean dataIntegrityStatus = true;
		// (Adj+/-1) interference constraints
		{
			ArrayList<Station> adjUpperChannelStations = new ArrayList<Station>();
			ArrayList<Station> adjLowerChannelStations = new ArrayList<Station>();
			//
			for (Station station : consideredStations) {
				long facilityId = station.getId();
				//
				adjUpperChannelStations.clear();
				adjUpperChannelStations.addAll(station.getAdjUpperChannelStations());
				adjUpperChannelStations.retainAll(consideredStations);
				//
				adjLowerChannelStations.clear();
				adjLowerChannelStations.addAll(station.getAdjLowerChannelStations());
				adjLowerChannelStations.retainAll(consideredStations);
				// Test for look up completeness
				for (Station upperStation : adjUpperChannelStations) {
					if (upperStation.hasAdjLowerChannelStation(station) == false) {
						dataIntegrityStatus = false;
						ConstrsGenrDriver.displayErr("Error: Missing the reverse ADJ-1 edge information from " + upperStation + " to " + station);
					}
				}
				// Start the constraints Adj-1
				if (adjLowerChannelStations.isEmpty() == false) {
					lineOut = adjBelowTag + "," + domainUB + "," + domainLB + "," + facilityId;
					//
					Collections.sort(adjLowerChannelStations, Comparators.StationByAscFacilityId);
					for (Station adjStation : adjLowerChannelStations) {
						long adjFacilityId = adjStation.getId();
						lineOut = lineOut + "," + adjFacilityId;
					}
					outStream.println(lineOut);
				}
				// Start the constraints Ad+1
				if (adjUpperChannelStations.isEmpty() == false) {
					lineOut = adjAboveTag + "," + domainLB + "," + domainUB + "," + facilityId;
					//
					Collections.sort(adjUpperChannelStations, Comparators.StationByAscFacilityId);
					for (Station adjStation : adjUpperChannelStations) {
						long adjFacilityId = adjStation.getId();
						lineOut = lineOut + "," + adjFacilityId;
					}
					outStream.println(lineOut);
				}
			}
		}
		//
		outStream.close();
		//
		if (dataIntegrityStatus == false) {
			ConstrsGenrDriver.displayErr("Error: Data Constraints completeness problem, see the violation(s) above");
			throw new Exception("Error: Data Constraint completeness problem, thrown from " + this.getClass().getSimpleName() + ".appendAdjChannelInterferencePairedCSV()");
		}
	}
	protected void appendCoChannelInterferencePairedCSV(ArrayList<Station> consideredStations, Channel channel) throws Exception {
		PrintWriter outStream = new PrintWriter(new FileWriter(new File("./output/Interference_Paired.csv"), true), true);
		Collections.sort(consideredStations, Comparators.StationByAscFacilityId);
		//
		String coTag = "CO";
		long domainLB = channel.getChannelNum();
		long domainUB = channel.getChannelNum();
		String lineOut;
		// CoChannel interference constraints
		{
			ArrayList<Station> adjStations = new ArrayList<Station>();
			for (Station station : consideredStations) {
				long facilityId = station.getId();
				//
				adjStations.clear();
				if (station.getCoChannelStations(channel) != null) {
					adjStations.addAll(station.getCoChannelStations(channel));
				}
				adjStations.retainAll(consideredStations);
				//
				lineOut = coTag + "," + domainLB + "," + domainUB + "," + facilityId;
				//
				Collections.sort(adjStations, Comparators.StationByAscFacilityId);
				for (Station adjStation : adjStations) {
					long adjFacilityId = adjStation.getId();
					lineOut = lineOut + "," + adjFacilityId;
				}
				outStream.println(lineOut);
			}
		}
		//
		outStream.close();
	}
	protected void appendDomainCSV(ArrayList<Station> consideredStations) throws Exception {
		PrintWriter outStream = new PrintWriter(new FileWriter(new File("./output/Domain.csv"), true), true);
		Collections.sort(consideredStations, Comparators.StationByAscFacilityId);
		//
		int highestChannelNum = this.profile.getIntegerParam(Profile.IntegerParamName.highest_channel_num);
		String lineOut;
		for (Station station : consideredStations) {
			if (station.hasPossible(station.getOrigChannel()) == false) {
				outStream.close();
				System.err.println("Error: " + station + " does not have its operating " + station.getOrigChannel() + " available");
				throw new Exception("Error: " + station + " does not have its operating " + station.getOrigChannel() + " available");
			}
			long facilityId = station.getId();
			//
			lineOut = "DOMAIN," + facilityId;
			//
			for (Channel channel : station.getPossibleChannels()) {
				if (channel.getChannelNum() <= highestChannelNum) {
					lineOut = lineOut + "," + channel.getChannelNum();
				}
			}
			outStream.println(lineOut);
		}
		outStream.close();
	}
	protected void compileConstrs() throws Exception {
		try {
			ArrayList<Station> consideredStations = new ArrayList<Station>(this.stationId2ConsideredStation.values());
			Collections.sort(consideredStations, Comparators.StationByAscFacilityId);
			ConstrsGenrDriver.displayLog();
			// clear output folder
			{
				File outputFolder = new File("./output");
				if (outputFolder.exists() && outputFolder.isDirectory()) {
					File[] files = outputFolder.listFiles();
					for (File file : files) {
						this.deleteFileFolder(file.getAbsolutePath());
					}
				}
				else {
					outputFolder.mkdir();
				}
				//
				{
					PrintWriter outStream;
					outStream = new PrintWriter(new FileWriter(new File("./output/Domain.csv"), false), true);
					outStream.close();
					outStream = new PrintWriter(new FileWriter(new File("./output/Interference_Paired.csv"), false), true);
					outStream.close();
				}
			}
			//
			int lowestChannelNum = profile.getIntegerParam(Profile.IntegerParamName.lowest_channel_num);
			int highestChannelNum = profile.getIntegerParam(Profile.IntegerParamName.highest_channel_num);
			double ignoredInterfPctUB = profile.getDoubleParam(Profile.DoubleParamName.nationwide_acceptable_interference_pct);
			Channel lowerChannel = null;
			Channel upperChannel = null;
			for (int lowerChannelNum = lowestChannelNum; lowerChannelNum < highestChannelNum; ++lowerChannelNum) {
				lowerChannel = Channel.getChannel(new Long(lowerChannelNum));
				int upperChannelNum = lowerChannelNum + 1;
				upperChannel = Channel.getChannel(new Long(upperChannelNum));
				// Band gap, skip
				if ( (lowerChannelNum == 4 && upperChannelNum == 5)
						|| (lowerChannelNum == 6 && upperChannelNum == 7)
						|| (lowerChannelNum == 13 && upperChannelNum == 14)
						|| (lowerChannelNum == 36 && upperChannelNum == 37)
						|| (lowerChannelNum == 37 && upperChannelNum == 38) ) {
					continue;
				}
				//
				ConstrsGenrDriver.displayLog("Processing data from consecutive channels [" + lowerChannel.getChannelNum() + "," + upperChannel.getChannelNum() + "]");
				//
				ConstrsGenrDriver.displayLog("\tReset and recycle attributes for consecutive channels [" + lowerChannel.getChannelNum() + "," + upperChannel.getChannelNum() + "]");
				Station.resetStationsAdjacencyGraph(lowerChannel, upperChannel);
				//
				ConstrsGenrDriver.displayLog("\tRead " + profile.getStringParam(Profile.StringParamName.tvstudy_interference_tablename) + " table in the TV_MODEL database");
				ConstrsGenrDriver.displayLog("\tWith acceptable additional pairwise interference pop percentage [" + ignoredInterfPctUB + "] between channel [" + lowerChannel.getChannelNum() + "," + upperChannel.getChannelNum() + "]");
				this.readTVSoftwarePairwiseResultChannelSpecific(lowerChannel, upperChannel, ignoredInterfPctUB, marginError);
				//
				ConstrsGenrDriver.displayLog();
				ConstrsGenrDriver.displayLog("\tVerify the interference data consistency and completeness");
				if (this.validateInterferencePairs(consideredStations, lowerChannel, upperChannel) == false) {
					ConstrsGenrDriver.displayLog("Error: \tThe interference data does not pass the consistency and completeness validation");
					throw new Exception("Error: \tInterference data consistency problem, thrown from " + this.getClass().getSimpleName() + ".compileConstrs()");
				}
				//
				// Generate Interference_Paired.csv Constraints
				{
					ConstrsGenrDriver.displayLog("\tAppend Interference_Paired.csv for range [" + lowerChannelNum + "," + upperChannelNum + "]");
					ConstrsGenrDriver.displayLog();
					if (lowerChannelNum == lowestChannelNum
							|| lowerChannelNum == 2
							|| (/*consideredGapBetween4And5 &&*/ lowerChannelNum == 5)
							|| lowerChannelNum == 7
							|| lowerChannelNum == 14
							|| lowerChannelNum == 38) {
						this.appendCoChannelInterferencePairedCSV(consideredStations, lowerChannel);
					}
					this.appendAdjChannelInterferencePairedCSV(consideredStations, lowerChannel, upperChannel);
					//
					this.appendCoChannelInterferencePairedCSV(consideredStations, upperChannel);
				}
			}
			// Write out Domain.csv
			{
				Collections.sort(consideredStations, Comparators.StationByAscFacilityId);
				ConstrsGenrDriver.displayLog("\tAppend out Domain.csv");
				this.appendDomainCSV(consideredStations);
				ConstrsGenrDriver.displayLog();
			}
		}
		catch (Exception ex) {
			ConstrsGenrDriver.displayLog(ex.getMessage());
			throw ex;
		}
	}
	private void delete(File file) {
		// Source: http://stackoverflow.com/users/3017357/indranil-bharambe
		if (file.isDirectory()) {
			String fileList[] = file.list();
			if (fileList.length == 0) {
				file.delete();
			}
			else {
				int size = fileList.length;
				for (int i = 0 ; i < size ; i++) {
					String fileName = fileList[i];
					String fullPath = file.getPath() + "/" + fileName;
					File fileOrFolder = new File(fullPath);
					delete(fileOrFolder);
				}
			}
		}
		else {
			file.delete();
		}
	}
	public void deleteFileFolder(String path) {
		// Source: http://stackoverflow.com/users/3017357/indranil-bharambe
		File file = new File(path);
		if (file.exists()) {
			do {
				delete(file);
			} while(file.exists());
		}
	}
	protected void determineConsideredStations() throws Exception {
		// collect default considered stations: RepackUS FixedCA FixedMX
		stationId2ConsideredStation.clear();
		stationId2ConsideredStation.putAll(USStation.getStationId2CAStation());
		stationId2ConsideredStation.putAll(USStation.getStationId2DCStation());
		stationId2ConsideredStation.putAll(USStation.getStationId2DDStation());
		stationId2ConsideredStation.putAll(USStation.getStationId2DRStation());
		stationId2ConsideredStation.putAll(USStation.getStationId2DTStation());
		stationId2ConsideredStation.putAll(USStation.getStationId2LDStation());
		stationId2ConsideredStation.putAll(USStation.getStationId2TXStation());
		//
		stationId2ProtectedStation.clear();
		stationId2ProtectedStation.putAll(USStation.getStationId2LMStation());
		stationId2ProtectedStation.putAll(USStation.getStationId2LMWStation());
		//
		stationId2ProtectedStation.putAll(CanadaStation.getStationId2DTCanadaStation());
		//stationId2ProtectedStation.putAll(CanadaStation.getStationId2LDCanadaStation());
		stationId2ProtectedStation.putAll(CanadaStation.getStationId2TACanadaStation());
		stationId2ProtectedStation.putAll(CanadaStation.getStationId2TVCanadaStation());
		//stationId2ProtectedStation.putAll(CanadaStation.getStationId2TXCanadaStation());
		//
		stationId2ProtectedStation.putAll(MexicoStation.getStationId2DTMexicoStation());
		//stationId2ProtectedStation.putAll(MexicoStation.getStationId2LDMexicoStation());
		stationId2ProtectedStation.putAll(MexicoStation.getStationId2TAMexicoStation());
		stationId2ProtectedStation.putAll(MexicoStation.getStationId2TVMexicoStation());
		//stationId2ProtectedStation.putAll(MexicoStation.getStationId2TXMexicoStation());
	}
	public void genrConstrs() throws Exception {
		try {
			ConstrsGenrDriver.displayLog("Compile the TV Study interference data to compute Domain and Constraints...");
			compileConstrs();
		}
		catch (Exception ex) {
			ConstrsGenrDriver.displayErr(ex.getMessage());
			ConstrsGenrDriver.displayErr(ex.getStackTrace().toString());
			throw ex;
		}
	}
	protected void initAbstractLevel() throws Exception {
		// initialize fixed parameters
		this.profile.addProtectedChannel(37);// nationwide
		this.profile.addProtectedChannel("HI",17);// only for Hawaii
		this.profile.addExcludedState("PR");// exclude Puerto Rico
		this.profile.addExcludedState("GU");// exclude Guam
		this.profile.addExcludedState("VI");// exclude Virgin Island
		this.profile.setParam("lowest_channel_num", "2");// lowest channel = 2
		this.profile.setParam("highest_channel_num", "51");// highest channel = 51
		// instantiate channels
		ConstrsGenrDriver.displayLog("Instantiate considered channel bounds:[" + Channel.minTvChannelNum + "," + Channel.maxTvChannelNum + "].");
		for (long channelNum = Channel.minTvChannelNum; channelNum <= Channel.maxTvChannelNum; ++channelNum) {
			Channel.acquireChannel(channelNum);
		}
		ConstrsGenrDriver.displayLog("Assign adjacent channels for each considered channel");
		Channel.setAdjChannels2ConsideredChannels();
		// Consider a gap between channel 4 and 5
		{
			Channel channel_4 = Channel.getChannel(4);
			channel_4.setAdjChannelAbove(null);
			Channel channel_5 = Channel.getChannel(5);
			channel_5.setAdjChannelBelow(null);
		}
		//
		this.readStations();
		//
		ConstrsGenrDriver.displayLog("Determine considered and protected stations");
		this.determineConsideredStations();
		//
		ConstrsGenrDriver.displayLog("Remove national and state protected channels");
		this.removeProtectedChannels();
		//
		this.pruneDomainDue2DistanceProtections();
	}
	protected boolean isCurrentlyShortSpaceOnAdjChannelAbove(Station studiedStation, Station interferingStation, Channel studiedChannel) throws Exception {
		Channel studiedStationOrigChannel = studiedStation.getOrigChannel();
		Channel interferingStationOrigChannel = interferingStation.getOrigChannel();
		String studiedStationOrigChannelSubBand = Channel.getBand(studiedStationOrigChannel);
		String studiedChannelSubBand = Channel.getBand(studiedChannel);
		// Currently CoChannel short-space stations are allowed to short-space on AdjChannels
		if (studiedStationOrigChannel.equals(interferingStationOrigChannel)) {
			// Short-spacing relaxation only allowed in the stations' original band
			if (studiedStationOrigChannelSubBand.equalsIgnoreCase(studiedChannelSubBand)) {
				return true;
			}
		}
		// Test whether studiedStation and interferingStation are currently adjacent, and interferingStation is above studiedStation at the same band as studiedChannel
		Channel studiedStationOrigChannelAbove = studiedStationOrigChannel.getAdjChannelAbove();
		if (studiedStationOrigChannelAbove != null && studiedStationOrigChannelAbove.equals(interferingStationOrigChannel)) {
			// In band Short-spacing relaxation
			if (studiedStationOrigChannelSubBand.equalsIgnoreCase(studiedChannelSubBand)) {
				return true;
			}
		}
		//
		return false;
	}
	protected boolean isCurrentlyShortSpaceOnAdjChannelBelow(Station studiedStation, Station interferingStation, Channel studiedChannel) throws Exception {
		Channel studiedStationOrigChannel = studiedStation.getOrigChannel();
		Channel interferingStationOrigChannel = interferingStation.getOrigChannel();
		String studiedStationOrigChannelSubBand = Channel.getBand(studiedStationOrigChannel);
		String studiedChannelSubBand = Channel.getBand(studiedChannel);
		// Currently CoChannel short-space stations are allowed to short-space on AdjChannels
		if (studiedStationOrigChannel.equals(interferingStationOrigChannel)) {
			// Short-spacing relaxation only allowed in the stations' original band
			if (studiedStationOrigChannelSubBand.equalsIgnoreCase(studiedChannelSubBand)) {
				return true;
			}
		}
		// Test whether studiedStation and interferingStation are currently adjacent, and 
		// interferingStation is below studiedStation at the same band as studiedChannel
		Channel studiedStationOrigChannelBelow = studiedStationOrigChannel.getAdjChannelBelow();
		if (studiedStationOrigChannelBelow != null && studiedStationOrigChannelBelow.equals(interferingStationOrigChannel)) {
			// In band Short-spacing relaxation
			if (studiedStationOrigChannelSubBand.equalsIgnoreCase(studiedChannelSubBand)) {
				return true;
			}
		}
		//
		return false;
	}
	protected boolean isCurrentlyShortSpaceOnCoChannel(Station studiedStation, Station interferingStation, Channel studiedChannel) throws Exception {
		Channel studiedStationOrigChannel = studiedStation.getOrigChannel();
		Channel interferingStationOrigChannel = interferingStation.getOrigChannel();
		// Test whether studiedStation and interferingStation are currently CoChannel, and 
		// interferingStation and studiedStation operating channels are at the same band as studiedChannel
		if (studiedStationOrigChannel.equals(interferingStationOrigChannel)) {
			String studiedStationOrigChannelSubBand = Channel.getBand(studiedStationOrigChannel);
			String studiedChannelSubBand = Channel.getBand(studiedChannel);
			// In band Short-spacing relaxation
			if (studiedStationOrigChannelSubBand.equalsIgnoreCase(studiedChannelSubBand)) {
				return true;
			}
		}
		//
		return false;
	}
	protected abstract void pruneDomainDue2DistanceProtections() throws Exception;
	protected void readLmLmwInterferenceTable() throws Exception {
		try {
			String tableName = profile.getStringParam(Profile.StringParamName.lm_lmw_interference_tablename);
			if (tableName == null || tableName.length() == 0) {
				tableName = "ia_lm_lmw_interference_table";// default table name
			}
			//
			ArrayList<HashMap<String, Object>> records = db.read_LM_LMW_INTERFERENCE_TABLE(tableName);
			//
			boolean dataIntegrityStatus = true;
			for (HashMap<String, Object> record : records) {
				Long facilityID = (Long) record.get("facility_id");
				Long protChannelNum = (Long) record.get("prot_channel");
				Long protFacilityID = (Long) record.get("prot_facility_id");
				Long adjLevel = (Long) record.get("adj_level");
				//
				if (facilityID == null || protChannelNum == null || protFacilityID == null || adjLevel == null) {
					ConstrsGenrDriver.displayErr("Error: Incomplete data [" + record + "]");
					dataIntegrityStatus = false;
					continue;
				}
				//
				if (stationId2ConsideredStation.containsKey(facilityID) == false) {
					ConstrsGenrDriver.displayLog("Warning: The base US TV Station [" + facilityID + "] is not in the considered stations list");
					continue;
				}
				//
				if (stationId2ProtectedStation.containsKey(protFacilityID) == false) {
					ConstrsGenrDriver.displayLog("Warning: The intefering LM/LMW operation [" + protFacilityID + "] is not in the considered protected LM/LMW facilities");
					continue;
				}
				// USA station
				Station usTVStation = Station.findStation(facilityID);
				if (usTVStation == null) {
					ConstrsGenrDriver.displayLog("Warning: The intefering US TV Station [" + facilityID + "] is not in the considered stations list");
					continue;
				}
				Channel usTVStationOrigChannel = usTVStation.getOrigChannel();
				// LM/LMW operation station
				Station lmLmwOperation = Station.findStation(protFacilityID);
				if (lmLmwOperation == null) {
					ConstrsGenrDriver.displayLog("Warning: The protecting Land Mobile [" + protFacilityID + "] is not in the considered protected LM/LMW facilities");
					continue;
				}
				Channel lmLmwOperationOrigChannel = lmLmwOperation.getOrigChannel();
				// Protected channel
				Channel protChannel = Channel.getChannel(protChannelNum);
				if (protChannel == null) {
					ConstrsGenrDriver.displayLog("Warning: Channel [" + protChannelNum + "] is out of range");
					continue;
				}
				Channel lmLmwOperationChannelAtAdjLevel = lmLmwOperationOrigChannel.getAdjChannel(adjLevel);
				if (lmLmwOperationChannelAtAdjLevel != null && protChannel.equals(lmLmwOperationChannelAtAdjLevel) == false) {
					ConstrsGenrDriver.displayErr("Error: Inconsistent record [" + record + "] ==> on " + lmLmwOperation + " original channel " + lmLmwOperationOrigChannel + " with level [" + adjLevel + "] is not " + protChannel);
					dataIntegrityStatus = false;
					continue;
				}
				//
				Channel opositeProtChannel = lmLmwOperationOrigChannel.getAdjChannel(-adjLevel);
				//
				if (stationId2ConsideredStation.containsKey(facilityID) && stationId2ProtectedStation.containsKey(protFacilityID)) {
					if (adjLevel.intValue() == 0) {
						if (usTVStationOrigChannel.equals(lmLmwOperationOrigChannel)) {
							continue;
						}
						else {
							usTVStation.removeChannel(protChannel);
						}
					}
					else if (adjLevel.intValue() == -1 || adjLevel.intValue() == 1) {
						// Assume symmetry
						if (usTVStationOrigChannel.equals(lmLmwOperationOrigChannel)
								|| usTVStationOrigChannel.equals(protChannel)
								|| (opositeProtChannel != null && usTVStationOrigChannel.equals(opositeProtChannel))) {
							continue;
						}
						else {
							usTVStation.removeChannel(protChannel);
						}
					}
					else {
						ConstrsGenrDriver.displayErr("Error: Inconsistent record [" + record + "] ==> Adj-Channel level [" + adjLevel + "] is out of range for LM/LMW protection");
						dataIntegrityStatus = false;
						continue;
					}
				}
				else {
					ConstrsGenrDriver.displayErr("Error: Inconsistent record [" + record + "] ==> The interfering [" + usTVStation + "] and the protected [" + lmLmwOperation + "] positions are inverted");
					dataIntegrityStatus = false;
					continue;
				}
			}
			//
			if (dataIntegrityStatus == false) {
				ConstrsGenrDriver.displayErr("Error: LM_LMW_INTERFERENCE_TABLE data integrity problem, see the list above");
				throw new Exception("Error: Data integrity problem, thrown from " + this.getClass().getSimpleName() + ".readLmLmwInterferenceTable()");
			}
		}
		catch (Exception ex) {
			throw ex;
		}
	}
	protected void readLmStationsTable() throws Exception {
		String callsign = "N/A";
		String serviceType = "LM";
		String country = "US";
		String stateAbbr = null;
		//
		try {
			String tableName = profile.getStringParam(Profile.StringParamName.lm_station_tablename);
			if (tableName == null || tableName.length() == 0) {
				tableName = "ia_lm_master";
			}
			//
			ArrayList<HashMap<String, Object>> records = db.read_LAND_MOBILE(tableName);
			//
			boolean dataIntegrityStatus = true;
			for (HashMap<String, Object> record : records) {
				Long facilityID = (Long) record.get("facility_id");
				Long channelNum = (Long) record.get("fac_channel");
				//
				if (facilityID == null || channelNum == null) {
					ConstrsGenrDriver.displayErr("Error: Incomplete data [" + record + "]");
					dataIntegrityStatus = false;
					continue;
				}
				//
				if (channelNum < 2 && channelNum > 51) {
					ConstrsGenrDriver.displayErr("Error: Channel number [" + record + "] is out of range");
					dataIntegrityStatus = false;
					continue;
				}
				//
				Channel origChannel = Channel.acquireChannel(channelNum);
				USStation.createLMStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			}
			//
			if (dataIntegrityStatus == false) {
				ConstrsGenrDriver.displayErr("Error: LM facility data completness problem, see the violation(s) above");
				throw new Exception("Error: LM facility data completness problem, thrown from " + this.getClass().getSimpleName() + ".readLmStationsTable()");
			}
		}
		catch (Exception ex) {
			throw ex;
		}
	}
	protected void readLmwStationsTable() throws Exception {
		String callsign = "N/A";
		String serviceType = "LMW";
		String country = "US";
		String stateAbbr = null;
		try {
			String tableName = profile.getStringParam(Profile.StringParamName.lmw_station_tablename);
			if (tableName == null || tableName.length() == 0) {
				tableName = "ia_lmw_master";// default table name
			}
			//
			ArrayList<HashMap<String, Object>> records = db.read_LAND_MOBILE_WAIVERS(tableName);
			//
			boolean dataIntegrityStatus = true;
			for (HashMap<String, Object> record : records) {
				Long facilityID = (Long) record.get("facility_id");
				Long channelNum = (Long) record.get("fac_channel");
				//
				if (facilityID == null || channelNum == null) {
					ConstrsGenrDriver.displayErr("Error: Incomplete data [" + record + "]");
					dataIntegrityStatus = false;
					continue;
				}
				//
				if (channelNum < 2 && channelNum > 51) {
					ConstrsGenrDriver.displayErr("Error: Channel number [" + record + "] is out of range");
					dataIntegrityStatus = false;
					continue;
				}
				//
				Channel origChannel = Channel.acquireChannel(channelNum);
				USStation.createLMWStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
			}
			//
			if (dataIntegrityStatus == false) {
				ConstrsGenrDriver.displayErr("Error: LMW facility data completeness problem, see the violation(s) above");
				throw new Exception("Error: LMW facility data completeness problem, thrown from " + this.getClass().getSimpleName() + ".readLmwStationsTable()");
			}
		}
		catch (Exception ex) {
			throw ex;
		}
	}
	protected void readMxInterferenceTable() throws Exception {
		try {
			String tableName = profile.getStringParam(Profile.StringParamName.mx_interference_tablename);
			if (tableName == null || tableName.length() == 0) {
				tableName = "ia_mx_interference_table";// default table name
			}
			//
			ArrayList<HashMap<String, Object>> records = db.read_MX_INTERFERENCE_TABLE(tableName);
			//
			boolean dataIntegrityStatus = true;
			for (HashMap<String, Object> record : records) {
				Long facilityID = (Long) record.get("facility_id");
				Long protChannelNum = (Long) record.get("prot_channel");
				Long protFacilityID = (Long) record.get("prot_facility_id");
				Long adjLevel = (Long) record.get("adj_level");
				//
				if (facilityID == null || protChannelNum == null || protFacilityID == null || adjLevel == null) {
					ConstrsGenrDriver.displayErr("Error: Incomplete data [" + record + "]");
					dataIntegrityStatus = false;
					continue;
				}
				//
				if ((protChannelNum.longValue() == 4 && adjLevel.longValue() == -1)
						|| (protChannelNum.longValue() == 5 && adjLevel.longValue() == 1)
						|| (protChannelNum.longValue() == 6 && adjLevel.longValue() == -1)
						|| (protChannelNum.longValue() == 7 && adjLevel.longValue() == 1)
						|| (protChannelNum.longValue() == 13 && adjLevel.longValue() == -1)
						|| (protChannelNum.longValue() == 14 && adjLevel.longValue() == 1)) {
					ConstrsGenrDriver.displayErr("Error: Incorrect data [" + record + "]; Channel gap between bands problem");
					dataIntegrityStatus = false;
					continue;
				}
				//
				if (stationId2ConsideredStation.containsKey(facilityID) == false) {
					ConstrsGenrDriver.displayLog("Warning: The base US TV Station [" + facilityID + "] is not in the considered stations list");
					continue;
				}
				//
				if (stationId2ProtectedStation.containsKey(protFacilityID) == false) {
					ConstrsGenrDriver.displayLog("Warning: The intefering Mexico TV Station [" + protFacilityID + "] is not in the protected MX stations list");
					continue;
				}
				// USA station
				Station usTVStation = Station.findStation(facilityID);
				if (usTVStation == null) {
					ConstrsGenrDriver.displayLog("Warning: The intefering US TV Station [" + facilityID + "] is not in the considered stations list");
					continue;
				}
				Channel usTVStationOrigChannel = usTVStation.getOrigChannel();
				// MX station
				Station mxTVStation = Station.findStation(protFacilityID);
				if (mxTVStation == null) {
					ConstrsGenrDriver.displayLog("Warning: The protecting Mexico TV Station [" + protFacilityID + "] is not in protected MX stations list");
					continue;
				}
				Channel mxTVStationOrigChannel = mxTVStation.getOrigChannel();
				// Protected channel
				Channel protChannel = Channel.getChannel(protChannelNum);
				if (protChannel == null) {
					ConstrsGenrDriver.displayLog("Warning: Channel [" + protChannelNum + "] is not considered");
					continue;
				}
				Channel mxTVStationChannelAtAdjLevel = mxTVStationOrigChannel.getAdjChannel(adjLevel);
				if (mxTVStationChannelAtAdjLevel != null && protChannel.equals(mxTVStationChannelAtAdjLevel) == false) {
					ConstrsGenrDriver.displayErr("Error: Inconsistent record [" + record + "] ==> " + usTVStation + " with " + mxTVStation + " original channel " + mxTVStationOrigChannel + " with level [" + adjLevel + "] is not " + protChannel);
					dataIntegrityStatus = false;
					continue;
				}
				//
				Channel opositeProtChannel = mxTVStationOrigChannel.getAdjChannel(-adjLevel);
				//
				if (stationId2ConsideredStation.containsKey(facilityID) && stationId2ProtectedStation.containsKey(protFacilityID)) {
					if (adjLevel.intValue() == 0) {
						if (usTVStationOrigChannel.equals(mxTVStationOrigChannel)) {
							continue;
						}
						else {
							usTVStation.removeChannel(protChannel);
						}
					}
					else if (adjLevel.intValue() == -1 || adjLevel.intValue() == 1) {
						// Assume symmetry
						if (usTVStationOrigChannel.equals(mxTVStationOrigChannel)
								|| usTVStationOrigChannel.equals(protChannel)
								|| (opositeProtChannel != null && usTVStationOrigChannel.equals(opositeProtChannel))) {
							continue;
						}
						else {
							usTVStation.removeChannel(protChannel);
						}
					}
					else if (Channel.adjLevels.contains(adjLevel)) {
						//Ignore adj-14 and adj-15
						if (adjLevel == -14  || adjLevel == -15) {
							ConstrsGenrDriver.displayErr("Error: Inconsistent record [" + record + "] ==> Adj-Channel level [" + adjLevel + "] is ignored for MX protection");
							dataIntegrityStatus = false;
							continue;
						}
						//Assume symmetry
						if (usTVStationOrigChannel.equals(protChannel)
								|| (opositeProtChannel != null && usTVStationOrigChannel.equals(opositeProtChannel))) {
							continue;
						}
						else {
							usTVStation.removeChannel(protChannel);
						}
					}
					else {
						ConstrsGenrDriver.displayErr("Error: Inconsistent record [" + record + "] ==> Adj-Channel level [" + adjLevel + "] is not valid for MX protection");
						dataIntegrityStatus = false;
						continue;
					}
				}
				else {
					ConstrsGenrDriver.displayErr("Error: Inconsistent record [" + record + "] ==> The interfering [" + usTVStation + "] and the protected [" + mxTVStation + "] positions are inverted");
					dataIntegrityStatus = false;
					continue;
				}
			}
			//
			if (dataIntegrityStatus == false) {
				ConstrsGenrDriver.displayErr("Error: MX_INTERFERENCE_TABLE data completeness problem, see the violation(s) above");
				throw new Exception("Error: MX_INTERFERENCE_TABLE data completeness problem, thrown from " + this.getClass().getSimpleName() + ".readMxInterferenceTable()");
			}
		}
		catch (Exception ex) {
			throw ex;
		}
	}
	protected void readStations() throws Exception {
		ConstrsGenrDriver.displayLog("Read Station table in the TV Study database");
		this.readStationsTable();
		//
		ConstrsGenrDriver.displayLog("Read LM Station in the TV Study database");
		this.readLmStationsTable();
		//
		ConstrsGenrDriver.displayLog("Read LM Waivers Station in the TV Study database");
		this.readLmwStationsTable();
	}
	protected void readStationsTable() throws Exception {
		try {
			String tableName = profile.getStringParam(Profile.StringParamName.tv_stations_tablename);
			if (tableName == null || tableName.length() == 0) {
				tableName = "tvsoftware_stations";// default table name
			}
			//
			ArrayList<HashMap<String, Object>> records = db.read_STATION_TABLE(tableName);
			//
			boolean dataIntegrityStatus = true;
			for (HashMap<String, Object> record : records) {
				Long facilityID = (Long) record.get("facility_id");
				String callsign = (String) record.get("fac_callsign");
				Long channelNum = (Long) record.get("fac_channel");
				String serviceType = (String) record.get("fac_service");
				String stateAbbr = (String) record.get("stateabbr");
				String country = (String) record.get("country");
				//
				if (facilityID == null || channelNum == null || serviceType == null || country == null) {
					ConstrsGenrDriver.displayErr("Error: Incomplete data [" + record + "]");
					dataIntegrityStatus = false;
					continue;
				}
				if (channelNum < 2 && channelNum > 51) {
					ConstrsGenrDriver.displayErr("Error: Channel number [" + record + "] is out of range");
					dataIntegrityStatus = false;
					continue;
				}
				// Instantiate the original channel of this station if it has not been created
				Channel origChannel = Channel.acquireChannel(channelNum);
				//
				if (country.equalsIgnoreCase("US")) {
					// Exclude selected states, typical territories: GU, PR, VI
					if (profile.isExcluded(stateAbbr)) {
						this.excludedStationIds.add(facilityID);
						continue;
					}
					//
					if (serviceType.equalsIgnoreCase(Station.TVServiceType.CA)) {
						USStation.createCAStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
					}
					else if (serviceType.equalsIgnoreCase(Station.TVServiceType.DC)) {
						USStation.createDCStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
					}
					else if (serviceType.equalsIgnoreCase(Station.TVServiceType.DD)) {
						USStation.createDDStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
					}
					else if (serviceType.equalsIgnoreCase(Station.TVServiceType.DR)) {
						USStation.createDRStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
					}
					else if (serviceType.equalsIgnoreCase(Station.TVServiceType.DT)) {
						USStation.createDTStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
					}
					else if (serviceType.equalsIgnoreCase(Station.TVServiceType.LD)) {
						USStation.createLDStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
					}
					else if (serviceType.equalsIgnoreCase(Station.TVServiceType.LM)) {
						ConstrsGenrDriver.displayErr("Error: The LM facility [" + record + "] should have not existed in here");
						dataIntegrityStatus = false;
						continue;
					}
					else if (serviceType.equalsIgnoreCase(Station.TVServiceType.LMW)) {
						ConstrsGenrDriver.displayErr("Error: The LMW facility [" + record + "] should have not existed in here");
						dataIntegrityStatus = false;
						continue;
					}
					else if (serviceType.equalsIgnoreCase(Station.TVServiceType.TX)) {
						USStation.createTXStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
					}
					else {
						ConstrsGenrDriver.displayErr("Error: unknown service type [" + record + "] of an USA station.");
						dataIntegrityStatus = false;
						continue;
					}
				}
				else if (country.equalsIgnoreCase("CA")) {
					if (serviceType.equalsIgnoreCase(Station.TVServiceType.DT)) {
						CanadaStation.createDTCanadaStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
					}
					else if (serviceType.equalsIgnoreCase(Station.TVServiceType.LD)) {
						CanadaStation.createLDCanadaStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
					}
					else if (serviceType.equalsIgnoreCase(Station.TVServiceType.TA)) {
						CanadaStation.createTACanadaStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
					}
					else if (serviceType.equalsIgnoreCase(Station.TVServiceType.TV)) {
						CanadaStation.createTVCanadaStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
					}
					else if (serviceType.equalsIgnoreCase(Station.TVServiceType.TX)) {
						CanadaStation.createTXCanadaStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
					}
					else {
						ConstrsGenrDriver.displayErr("Error: unknown service type [" + record + "] of a Canadian station.");
						dataIntegrityStatus = false;
						continue;
					}
				}
				else if (country.equalsIgnoreCase("MX")) {
					if (serviceType.equalsIgnoreCase(Station.TVServiceType.DT)) {
						MexicoStation.createDTMexicoStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
					}
					else if (serviceType.equalsIgnoreCase(Station.TVServiceType.LD)) {
						MexicoStation.createLDMexicoStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
					}
					else if (serviceType.equalsIgnoreCase(Station.TVServiceType.TA)) {
						MexicoStation.createTAMexicoStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
					}
					else if (serviceType.equalsIgnoreCase(Station.TVServiceType.TV)) {
						MexicoStation.createTVMexicoStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
					}
					else if (serviceType.equalsIgnoreCase(Station.TVServiceType.TX)) {
						MexicoStation.createTXMexicoStation(facilityID, callsign, origChannel, serviceType, country, stateAbbr);
					}
					else {
						ConstrsGenrDriver.displayErr("Error: unknown service type [" + record + "] of a Mexican station.");
						dataIntegrityStatus = false;
						continue;
					}
				}
				else {
					ConstrsGenrDriver.displayErr("Error: unknown country code [" + record + "].");
					dataIntegrityStatus = false;
					continue;
				}
			}
			//
			if (dataIntegrityStatus == false) {
				ConstrsGenrDriver.displayErr("Error: tvsoftware_stations data completeness problem, see the violation(s) above");
				throw new Exception("Error: tvsoftware_stations data completeness problem, thrown from " + this.getClass().getSimpleName() + ".readStationsTable()");
			}
		}
		catch (Exception ex) {
			throw ex;
		}
	}
	protected void readTVSoftwarePairwiseResultChannelSpecific(Channel lowerChannel, Channel upperChannel, double ignoredInterfPctUB, double marginError) throws Exception {/* stub */}
	protected void removeProtectedChannels() throws Exception {
		HashSet<Long> nationwideProtectedChannelNums = profile.getProtectedChannelNums();
		for (Station station : stationId2ConsideredStation.values()) {
			// Nationwide protected channels
			if (nationwideProtectedChannelNums.isEmpty() == false) {
				for (Long chNum : nationwideProtectedChannelNums) {
					Channel channel = Channel.getChannel(chNum.longValue());
					station.removeChannel(channel);
				}
			}
			// By state
			String stateAbbr = station.getStateAbbr();
			HashSet<Long> stateProtectedChannelNums = profile.getProtectedChannelNums(stateAbbr);
			if (stateProtectedChannelNums != null) {
				for (Long chNum : stateProtectedChannelNums) {
					Channel channel = Channel.getChannel(chNum.longValue());
					station.removeChannel(channel);
				}
			}
		}
	}
	@Override
	public String toString() { return this.getClass().getName(); }
	protected boolean validateInterferencePairs(ArrayList<Station> consideredStations, Channel lowerChannel, Channel upperChannel) {
		boolean dataIntegrityStatus = true;
		// Validate the rule: "ADJ+/-1 implies CO"
		// To catch the TV Study error code 2 where there is an AdjChannel constraint without its associated CoChannel constraints 
		for (Station studyStation : consideredStations) {
			if (studyStation.getCountry().equalsIgnoreCase(Station.CountryName.MX)) {
				continue;
			}
			for (Station adjLowerChannelStation : studyStation.getAdjLowerChannelStations()) {
				if (adjLowerChannelStation.getCountry().equalsIgnoreCase(Station.CountryName.MX)) {
					continue;
				}
				if (adjLowerChannelStation.hasCoChannelStation(studyStation, lowerChannel) == false
						|| adjLowerChannelStation.hasCoChannelStation(studyStation, upperChannel) == false
						|| studyStation.hasCoChannelStation(adjLowerChannelStation, lowerChannel) == false
						|| studyStation.hasCoChannelStation(adjLowerChannelStation, upperChannel) == false) {
					ConstrsGenrDriver.displayErr("Warning: Missing a CoChannel pair [" + studyStation.getId() + "," + adjLowerChannelStation.getId() + "] between consecutive [" + lowerChannel + "," + upperChannel + "]");
					//
					ConstrsGenrDriver.displayErr("Handling: Added CoChannel pair [" + studyStation.getId() + "," + adjLowerChannelStation.getId() + "] on both consecutive [" + lowerChannel + "," + upperChannel + "]");
					studyStation.addCoChannelStation(lowerChannel, adjLowerChannelStation);
					studyStation.addCoChannelStation(upperChannel, adjLowerChannelStation);
					adjLowerChannelStation.addCoChannelStation(lowerChannel, studyStation);
					adjLowerChannelStation.addCoChannelStation(upperChannel, studyStation);
				}
				if (adjLowerChannelStation.hasAdjUpperChannelStation(studyStation) == false) {
					ConstrsGenrDriver.displayErr("Error: Missing the reverse pair [" + studyStation.getId() + "," + adjLowerChannelStation.getId() + "] between consecutive [" + lowerChannel + "," + upperChannel + "]");
					dataIntegrityStatus = false;
				}
			}
		}
		// Validate the short spacing rules: CoChannel and AdjChannel for every pair of considered stations
		Channel studyOrigChannel;
		Channel interfOrigChannel;
		for (Station studyStation : consideredStations) {
			studyOrigChannel = studyStation.getOrigChannel();
			if (studyOrigChannel.equals(lowerChannel)) {
				// CoChannel short spacing on lowerChannel
				for (Station interfCoChannelStation : studyStation.getCoChannelStations(lowerChannel)) {
					if (studyStation.getCountry().equalsIgnoreCase(Station.CountryName.MX) == false 
							&& interfCoChannelStation.getCountry().equalsIgnoreCase(Station.CountryName.MX) == false) {
						continue;
					}
					//
					interfOrigChannel = interfCoChannelStation.getOrigChannel();
					if (interfOrigChannel.equals(lowerChannel)) {
						ConstrsGenrDriver.displayErr("Warning: With option 2, these CoChannel short spaced stations [" + studyStation + "," + interfCoChannelStation + "]' for [" + lowerChannel + "] interference free pops should have null overlap/intersection");
						//
						ConstrsGenrDriver.displayErr("Handling: Remove these constraints");
						studyStation.removeCoChannelStation(studyOrigChannel, interfCoChannelStation);
						interfCoChannelStation.removeCoChannelStation(studyOrigChannel, studyStation);
					}
				}
				// AdjChannel above short spacing
				for (Station interfAdjChannelStation : studyStation.getAdjUpperChannelStations()) {
					if (studyStation.getCountry().equalsIgnoreCase(Station.CountryName.MX) == false 
							&& interfAdjChannelStation.getCountry().equalsIgnoreCase(Station.CountryName.MX) == false) {
						continue;
					}
					//
					interfOrigChannel = interfAdjChannelStation.getOrigChannel();
					if (interfOrigChannel.equals(upperChannel)) {
						ConstrsGenrDriver.displayErr("Warning: With option 2, these AdjChannel short spaced stations [" + studyStation + "," + interfAdjChannelStation + "]' for [" + lowerChannel + "," + upperChannel + "] interference free pops should have null overlap/intersection");
						//
						ConstrsGenrDriver.displayErr("Handling: Remove these constraints");
						studyStation.removeAdjUpperChannelStation(interfAdjChannelStation);
						interfAdjChannelStation.removeAdjLowerChannelStation(studyStation);
					}
				}
			}
			//
			if (studyOrigChannel.equals(upperChannel)) {
				// CoChannel short spacing on upperChannel
				for (Station interfCoChannelStation : studyStation.getCoChannelStations(upperChannel)) {
					if (studyStation.getCountry().equalsIgnoreCase(Station.CountryName.MX) == false 
							&& interfCoChannelStation.getCountry().equalsIgnoreCase(Station.CountryName.MX) == false) {
						continue;
					}
					//
					interfOrigChannel = interfCoChannelStation.getOrigChannel();
					if (interfOrigChannel.equals(upperChannel)) {
						ConstrsGenrDriver.displayErr("Warning: With option 2, these CoChannel short spaced stations [" + studyStation + "," + interfCoChannelStation + "]' for [" + upperChannel + "] interference free pops should have null overlap/intersection");
						//
						ConstrsGenrDriver.displayErr("Handling: Remove these constraints");
						studyStation.removeCoChannelStation(studyOrigChannel, interfCoChannelStation);
						interfCoChannelStation.removeCoChannelStation(studyOrigChannel, studyStation);
					}
				}
				// AdjChannel below short spacing
				for (Station interfAdjChannelStation : studyStation.getAdjLowerChannelStations()) {
					if (studyStation.getCountry().equalsIgnoreCase(Station.CountryName.MX) == false 
							&& interfAdjChannelStation.getCountry().equalsIgnoreCase(Station.CountryName.MX) == false) {
						continue;
					}
					//
					interfOrigChannel = interfAdjChannelStation.getOrigChannel();
					if (interfOrigChannel.equals(lowerChannel)) {
						ConstrsGenrDriver.displayErr("Warning: With option 2, these AdjChannel short spaced stations [" + studyStation + "," + interfAdjChannelStation + "]' for [" + upperChannel + "," + lowerChannel + "] interference free pops should have null overlap/intersection");
						//
						ConstrsGenrDriver.displayErr("Handling: Remove these constraints");
						studyStation.removeAdjLowerChannelStation(interfAdjChannelStation);
						interfAdjChannelStation.removeAdjUpperChannelStation(studyStation);
					}
				}
			}
		}
		//
		if (dataIntegrityStatus == false) {
			ConstrsGenrDriver.displayErr("Error: Found interference data completeness problem, see the violation(s) above");
			return false;
		}
		//
		return dataIntegrityStatus;
	}
}

