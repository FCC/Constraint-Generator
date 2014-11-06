package com.computechinc.tvmodel.models;

import java.util.*;

import com.computechinc.tvmodel.*;
import com.computechinc.tvmodel.containers.*;

public class CS_RepackUSRepackCAFixedMX_Option2 extends AbstractConstrsGenr {
	//========================================================================//
	// Class level
	//========================================================================//

	//========================================================================//
	// Instance level
	//========================================================================//
	// local attributes
	protected Double marginError = new Double(0.0001);
	// constructors
	public CS_RepackUSRepackCAFixedMX_Option2(Profile profile) throws Exception {
		super(profile);
		//
		init();
	}
	//
	// methods

	protected void determineConsideredStations() throws Exception {
		// collect considered stations
		super.stationId2ConsideredStation.clear();
		super.stationId2ConsideredStation.putAll(USStation.getStationId2CAStation());
		super.stationId2ConsideredStation.putAll(USStation.getStationId2DCStation());
		super.stationId2ConsideredStation.putAll(USStation.getStationId2DDStation());
		super.stationId2ConsideredStation.putAll(USStation.getStationId2DRStation());
		super.stationId2ConsideredStation.putAll(USStation.getStationId2DTStation());
		super.stationId2ConsideredStation.putAll(USStation.getStationId2LDStation());
		super.stationId2ConsideredStation.putAll(USStation.getStationId2TXStation());
		//
		super.stationId2ConsideredStation.putAll(CanadaStation.getStationId2DTCanadaStation());
		//super.stationId2ConsideredStation.putAll(CanadaStation.getStationId2LDCanadaStation());
		super.stationId2ConsideredStation.putAll(CanadaStation.getStationId2TACanadaStation());
		super.stationId2ConsideredStation.putAll(CanadaStation.getStationId2TVCanadaStation());
		//super.stationId2ConsideredStation.putAll(CanadaStation.getStationId2TXCanadaStation());
		//
		{
			// Distribute considered stations into band buckets
			for (Station station : stationId2ConsideredStation.values()) {
				Channel origChannel = station.getOrigChannel();
				String origBand = Channel.getBand(origChannel);
				if (origBand.equalsIgnoreCase(Channel.UHF)) {
					this.consideredUHFStations.add(station);
				}
				else if (origBand.equalsIgnoreCase(Channel.UpperVHF)) {
					this.consideredUpperVHFStations.add(station);
				}
				else if (origBand.equalsIgnoreCase(Channel.LowerVHF)) {
					this.consideredLowerVHFStations.add(station);
				}
				else {
					ConstrsGenrDriver.displayLog("Warning: Ignore unknown orig band [" + origBand + "] of " + station);
					continue;
				}
			}
		}
		//
		super.stationId2ProtectedStation.clear();
		super.stationId2ProtectedStation.putAll(USStation.getStationId2LMStation());
		super.stationId2ProtectedStation.putAll(USStation.getStationId2LMWStation());
		//
		super.stationId2ProtectedStation.putAll(MexicoStation.getStationId2DTMexicoStation());
		//super.stationId2ProtectedStation.putAll(MexicoStation.getStationId2LDMexicoStation());
		super.stationId2ProtectedStation.putAll(MexicoStation.getStationId2TAMexicoStation());
		super.stationId2ProtectedStation.putAll(MexicoStation.getStationId2TVMexicoStation());
		//super.stationId2ProtectedStation.putAll(MexicoStation.getStationId2TXMexicoStation());
	}
	protected void pruneDomainDue2DistanceProtections() throws Exception {
		ConstrsGenrDriver.displayLog("Read LM_LMW_INTERFERENCE_TABLE in the TV Study database to protect considered LM/LMW facilities");
		super.readLmLmwInterferenceTable();
		//
		ConstrsGenrDriver.displayLog("Read MX_INTERFERENCE_TABLE in the TV Study database to protect Mexico stations");
		super.readMxInterferenceTable();
	}
	private void init() throws Exception {
		// skip
	}
	protected void readTVSoftwarePairwiseResultChannelSpecific(Channel lowerChannel, Channel upperChannel, double ignoredInterfPctUB, double marginError) throws Exception {
		Long studiedFacilityId;
		Long studiedChannelNum;
		Long interferingFacilityId;
		Long interferingChannelNum;
		Double interferedServedPopPct;
		long lowerChNum = lowerChannel.getChannelNum();
		long upperChNum = upperChannel.getChannelNum();
		//
		try {
			String tableName = profile.getStringParam(Profile.StringParamName.tvstudy_interference_tablename);
			if (tableName == null || tableName.length() == 0) {
				tableName = "tvsoftware_pairwise_result_final";// default table name
			}
			//
			ArrayList<HashMap<String, Object>> records = db.read_TVSOFTWARE_PAIRWISE_RESULT_FINAL_TABLE(tableName, lowerChannel.getChannelNum(), marginError);
			//
			boolean dataIntegrityStatus = true;
			for (HashMap<String, Object> record : records) {
				studiedFacilityId = (Long) record.get("orig_facilityid");
				studiedChannelNum = (Long) record.get("orig_channel");
				interferingFacilityId = (Long) record.get("interferingfacilityid");
				interferingChannelNum = (Long) record.get("interferingchannel");
				interferedServedPopPct = (Double) record.get("interference_pct");
				//
				if (studiedFacilityId == null || studiedChannelNum == null || interferingFacilityId == null
						|| interferingChannelNum == null || interferedServedPopPct == null) {
					ConstrsGenrDriver.displayErr("Error: Incomplete data [" + record + "]");
					dataIntegrityStatus = false;
					continue;
				}
				//
				Station studiedStation = Station.findStation(studiedFacilityId); 
				if (studiedStation == null) {
					if (super.excludedStationIds.contains(studiedFacilityId) == false) {
						ConstrsGenrDriver.displayLog("Warning: The studied TV Station [" + studiedFacilityId + "] is currently not considered");
					}
					continue;
				}
				Station interferingStation = Station.findStation(interferingFacilityId);
				if (interferingStation == null) {
					if (super.excludedStationIds.contains(interferingFacilityId) == false) {
						ConstrsGenrDriver.displayLog("Warning: The interfering Station [" + interferingFacilityId + "] is currently not considered");
					}
					continue;
				}
				Channel studiedChannel = Channel.getChannel(studiedChannelNum);
				Channel interferingChannel = Channel.getChannel(interferingChannelNum);
				if (studiedChannel == null || interferingChannel == null) {
					ConstrsGenrDriver.displayErr("Error: Inconsistent record [" + record + "] ==> Unknown interfering channel [" + studiedChannelNum + "]; and protected channel [" + interferingChannelNum + "]");
					dataIntegrityStatus = false;
					continue;
				}
				Channel studiedOrigChannel = studiedStation.getOrigChannel();
				Channel interferingOrigChannel = interferingStation.getOrigChannel();
				//
				if (stationId2ConsideredStation.containsKey(studiedFacilityId) && stationId2ConsideredStation.containsKey(interferingFacilityId)) {
					if ( (studiedChannelNum == 4 && interferingChannelNum == 5)
							|| (studiedChannelNum == 5 && interferingChannelNum == 4)
							|| (studiedChannelNum == 6 && interferingChannelNum == 7)
							|| (studiedChannelNum == 7 && interferingChannelNum == 6)
							|| (studiedChannelNum == 13 && interferingChannelNum == 14)
							|| (studiedChannelNum == 14 && interferingChannelNum == 13) ) {
						//
						ConstrsGenrDriver.displayErr("Error: Incorrect data; channel gap [" + lowerChNum + "," + upperChNum + "] on pair [" + studiedStation + "," + interferingStation + "," + interferedServedPopPct + "%] for Option 2");
						dataIntegrityStatus = false;
						continue;
					}
					//
					if ((lowerChNum == studiedChannelNum.longValue() && studiedChannelNum.longValue() == interferingChannelNum.longValue())
							|| (upperChNum == studiedChannelNum.longValue() && studiedChannelNum.longValue() == interferingChannelNum.longValue()) ) {
						if (interferedServedPopPct.doubleValue() > ignoredInterfPctUB) {
							if (super.isCurrentlyShortSpaceOnCoChannel(studiedStation, interferingStation, studiedChannel) == false) {
								studiedStation.addCoChannelStation(studiedChannel, interferingStation);
								interferingStation.addCoChannelStation(studiedChannel, studiedStation);
							}
							else {
								studiedStation.addCoChannelStation(studiedChannel, interferingStation);
								interferingStation.addCoChannelStation(studiedChannel, studiedStation);
								//
								if (studiedChannel.equals(studiedOrigChannel) && studiedOrigChannel.equals(interferingOrigChannel)) {
									ConstrsGenrDriver.displayLog("Warning: Original CoChannel short-spacing violation: study station [" + studiedFacilityId + "] and interfering station [" + interferingFacilityId + "] are currently short spaced on operating channel [" + studiedChannelNum + "]");
								}
							}
						}
						else if (interferedServedPopPct.doubleValue() > marginError) {
							// skip when pct is in between marginError and ignored threshold 
						}
						else {
							// skip when pct is lower than or equal to marginError
						}
					}
					// Look up
					else if (lowerChNum == studiedChannelNum.longValue() && studiedChannelNum.longValue() + 1 == interferingChannelNum.longValue()) {
						if (interferedServedPopPct.doubleValue() > ignoredInterfPctUB) {
							if (super.isCurrentlyShortSpaceOnAdjChannelAbove(studiedStation, interferingStation, studiedChannel) == false) {
								studiedStation.addAdjUpperChannelStation(interferingStation);
								interferingStation.addAdjLowerChannelStation(studiedStation);
							}
							else {
								studiedStation.addAdjUpperChannelStation(interferingStation);
								interferingStation.addAdjLowerChannelStation(studiedStation);
								//
								if (studiedChannel.equals(studiedOrigChannel) && studiedOrigChannel.getAdjChannelAbove() != null && studiedOrigChannel.getAdjChannelAbove().equals(interferingOrigChannel)) {
									ConstrsGenrDriver.displayLog("Warning: Original AdjChannel short-spacing violation: study station [" + studiedFacilityId + "] and interfering station [" + interferingFacilityId + "] are currently short spaced on adjacent channels [" + studiedChannelNum + "," + interferingChannelNum + "]");
								}
							}
						}
						else if (interferedServedPopPct.doubleValue() > marginError) {
							// skip when pct is in between marginError and ignored threshold 
						}
						else {
							// skip when pct is lower than or equal to marginError
						}
					}
					// Look down
					else if (upperChNum == studiedChannelNum.longValue() && studiedChannelNum.longValue() - 1 == interferingChannelNum.longValue()) {
						if (interferedServedPopPct.doubleValue() > ignoredInterfPctUB) {
							if (super.isCurrentlyShortSpaceOnAdjChannelBelow(studiedStation, interferingStation, studiedChannel) == false) {
								studiedStation.addAdjLowerChannelStation(interferingStation);
								interferingStation.addAdjUpperChannelStation(studiedStation);						
							}
							else {
								studiedStation.addAdjLowerChannelStation(interferingStation);
								interferingStation.addAdjUpperChannelStation(studiedStation);						
								//
								if (studiedChannel.equals(studiedOrigChannel) && studiedOrigChannel.getAdjChannelBelow() != null && studiedOrigChannel.getAdjChannelBelow().equals(interferingOrigChannel)) {
									ConstrsGenrDriver.displayLog("Warning: Original AdjChannel short-spacing violation: study station [" + studiedFacilityId + "] and interfering station [" + interferingFacilityId + "] are currently short spaced on adjacent channels [" + studiedChannelNum + "," + interferingChannelNum + "]");
								}
							}
						}
						else if (interferedServedPopPct.doubleValue() > marginError) {
							// skip when pct is in between marginError and ignored threshold 
						}
						else {
							// skip when pct is lower than or equal to marginError
						}
					}
					else {
						// skip for channels distance larger than 1
						continue;
					}
				}
				else if (stationId2ConsideredStation.containsKey(studiedFacilityId) && stationId2ProtectedStation.containsKey(interferingFacilityId)) {
					if (interferingStation.getCountry().equalsIgnoreCase("CA") && interferingOrigChannel.equals(interferingChannel)) {
						ConstrsGenrDriver.displayErr("Error: Unexpected interference record: studied channels [" + studiedChannelNum + "] on stations pair [" + studiedStation + "," + studiedOrigChannel + "," + interferingStation + "," + interferingOrigChannel + "," + interferedServedPopPct + "%] for Option 2");
						dataIntegrityStatus = false;
						continue;
					}
					if (interferingStation.getCountry().equalsIgnoreCase("US") && interferingOrigChannel.equals(interferingChannel)) {
						ConstrsGenrDriver.displayErr("Error: Unexpected interference record: studied channels [" + studiedChannelNum + "] on stations pair [" + studiedStation + "," + studiedOrigChannel + "," + interferingStation + "," + interferingOrigChannel + "," + interferedServedPopPct + "%] for Option 2");
						dataIntegrityStatus = false;
						continue;
					}
				}
				else if (stationId2ConsideredStation.containsKey(interferingFacilityId) && stationId2ProtectedStation.containsKey(studiedFacilityId)) {
					if (studiedStation.getCountry().equalsIgnoreCase("CA") && studiedOrigChannel.equals(studiedChannel)) {
						ConstrsGenrDriver.displayErr("Error: Unexpected interference record: studied channels [" + studiedChannelNum + "] on stations pair [" + studiedStation + "," + studiedOrigChannel + "," + interferingStation + "," + interferingOrigChannel + "," + interferedServedPopPct + "%] for Option 2");
						dataIntegrityStatus = false;
						continue;
					}
					if (studiedStation.getCountry().equalsIgnoreCase("US") && studiedOrigChannel.equals(studiedChannel)) {
						ConstrsGenrDriver.displayErr("Error: Unexpected interference record: studied channels [" + studiedChannelNum + "] on stations pair [" + studiedStation + "," + studiedOrigChannel + "," + interferingStation + "," + interferingOrigChannel + "," + interferedServedPopPct + "%] for Option 2");
						dataIntegrityStatus = false;
						continue;
					}
				}
				else {
					continue;
				}
			}
			//
			if (dataIntegrityStatus == false) {
				ConstrsGenrDriver.displayErr("Error: " + tableName + " data integrity problem, see the list above");
				throw new Exception("Error: Data integrity problem, thrown from " + this.getClass().getSimpleName() + ".readTVSoftwarePairwiseResultChannelSpecific()");
			}
		}
		catch (Exception ex) {
			throw ex;
		}
	}
	@Override
	public String toString() { return this.getClass().getName(); }
}

