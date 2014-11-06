package com.computechinc.tvmodel.containers;

import java.util.*;

import com.computechinc.tvmodel.ConstrsGenrDriver;

public abstract class Station {
	//========================================================================//
	// Class level
	//========================================================================//
	public static class CountryName {
		public static String CA = "CA";
		public static String MX = "MX";
		public static String US = "US";
	}
	public static class TVServiceType {
		public static String CA = "CA";
		public static String DC = "DC";
		public static String DD = "DD";
		public static String DR = "DR";
		public static String DT = "DT";
		public static String LD = "LD";
		public static String LM = "LM";
		public static String LMW = "LMW";
		public static String TA = "TA";
		public static String TV = "TV";
		public static String TX = "TX";
	}
	private static HashMap<Long,Station> stationId2Station = new HashMap<Long,Station>();
	protected static void addStation(Station newStation) {
		if (stationId2Station.get(newStation.getId()) == null) {
			stationId2Station.put(newStation.getId(), newStation);
			return;
		}
		ConstrsGenrDriver.displayLog("Warning: Found stations with duplicate ID [" + newStation + "].");
	}
	public static Station findStation(Long facilityID) { return stationId2Station.get(facilityID); }
	public static HashMap<Long,Station> getStationId2Station() { return new HashMap<Long,Station>(stationId2Station); }
	public static void resetStationsAdjacencyGraph(Channel lowerChannel, Channel upperChannel) throws Exception {
		for (Station station : stationId2Station.values()) {
			station.resetAdjacencyGraph(lowerChannel, upperChannel);
		}
	}
	//
	//========================================================================//
	// Instance level
	//========================================================================//
	protected long id;
	protected String callsign;
	protected Channel origChannel;
	protected String serviceType;
	protected String country;
	protected String stateabbr;
	protected ArrayList<Channel> possibleChannels = new ArrayList<Channel>();
	// containers for stations which cannot be CoChannel and/or AdjChannel with this station for the given pct interference threshold
	protected LinkedHashMap<Channel,HashSet<Station>> orderedPairChannel2CoChannelStations = new LinkedHashMap<Channel,HashSet<Station>>(); 
	protected HashSet<Station> adjLowerChannelStations = new HashSet<Station>();
	protected HashSet<Station> adjUpperChannelStations = new HashSet<Station>();
	//
	// constructors
	protected Station(Long facilityID, String callsign, Channel origChannel, String serviceType, String country, String stateabbr) throws Exception {
		this.id = facilityID.longValue();
		this.callsign = callsign;
		this.origChannel = origChannel;
		this.serviceType = serviceType;
		this.country = country;
		this.stateabbr = stateabbr;
		// by default every station can occupy any considered channel
		this.possibleChannels = Channel.getTVChannels();
		Station.addStation(this);
	}
	//
	// methods

	public void addAdjLowerChannelStation(Station protStation) { this.adjLowerChannelStations.add(protStation); }
	public void addAdjUpperChannelStation(Station protStation) { this.adjUpperChannelStations.add(protStation); }
	public void addCoChannelStation(Channel studiedChannel, Station protStation) {
		HashSet<Station> coChannelStations = orderedPairChannel2CoChannelStations.get(studiedChannel);
		if (coChannelStations == null) {
			coChannelStations = new HashSet<Station>();
			orderedPairChannel2CoChannelStations.put(studiedChannel,coChannelStations);
		}
		coChannelStations.add(protStation);
	}
	public boolean equals(Station aStation) { return this.id == aStation.getId(); }
	public HashSet<Station> getAdjLowerChannelStations() { return new HashSet<Station>(this.adjLowerChannelStations); }
	public int getAdjLowerChannelStationsSize() { return this.adjLowerChannelStations.size(); }
	public HashSet<Station> getAdjUpperChannelStations() { return new HashSet<Station>(this.adjUpperChannelStations); }
	public int getAdjUpperChannelStationsSize() { return this.adjUpperChannelStations.size(); }
	public String getCallsign() { return callsign; }
	public HashSet<Station> getCoChannelStations(Channel channel) {
		HashSet<Station> coChannelStations = new HashSet<Station>();
		if (this.orderedPairChannel2CoChannelStations.get(channel) != null) {
			coChannelStations.addAll(this.orderedPairChannel2CoChannelStations.get(channel));
		}
		//
		return coChannelStations;
	}
	public int getCoChannelStationsSizeAtLowerChannel() {
		ArrayList<Channel> pairChannels = new ArrayList<Channel>(this.orderedPairChannel2CoChannelStations.keySet());
		return orderedPairChannel2CoChannelStations.get(pairChannels.get(0)).size();
	}
	public int getCoChannelStationsSizeAtUpperChannel() {
		ArrayList<Channel> pairChannels = new ArrayList<Channel>(this.orderedPairChannel2CoChannelStations.keySet());
		return orderedPairChannel2CoChannelStations.get(pairChannels.get(1)).size();
	}
	public String getCountry() { return country; }
	public long getId() { return id; }
	public Channel getOrigChannel() { return origChannel; }
	public long getOrigChannelNum() { return origChannel.getChannelNum(); }
	public ArrayList<Channel> getPossibleChannels() { return new ArrayList<Channel>(this.possibleChannels); }
	public String getServiceType() { return serviceType; }
	public String getStateAbbr() { return this.stateabbr; }
	public boolean hasAdjLowerChannelStation(Station adjLowerStation) { return this.adjLowerChannelStations.contains(adjLowerStation); }
	public boolean hasAdjUpperChannelStation(Station adjUpperStation) { return this.adjUpperChannelStations.contains(adjUpperStation); }
	public boolean hasCoChannelStation(Station coStation, Channel channel) { return this.orderedPairChannel2CoChannelStations.get(channel).contains(coStation); }
	public boolean hasPossible(Channel channel) { return this.possibleChannels.contains(channel); }
	public void removeAdjLowerChannelStation(Station adjLowerStation) { this.adjLowerChannelStations.remove(adjLowerStation); }
	public void removeAdjUpperChannelStation(Station adjUpperStation) { this.adjUpperChannelStations.remove(adjUpperStation); }
	public void removeChannel(Channel channel) {
		if (this.possibleChannels.contains(channel)) {
			this.possibleChannels.remove(channel);
		}
	}
	public void removeCoChannelStation(Channel channel, Station coStation) { this.orderedPairChannel2CoChannelStations.get(channel).remove(coStation); }
	public void resetAdjacencyGraph(Channel lowerChannel, Channel upperChannel) {
		this.orderedPairChannel2CoChannelStations.clear();
		this.orderedPairChannel2CoChannelStations.put(lowerChannel, new HashSet<Station>());
		this.orderedPairChannel2CoChannelStations.put(upperChannel, new HashSet<Station>());
		this.adjLowerChannelStations.clear();
		this.adjUpperChannelStations.clear();
	}
	//
	@Override
	public String toString() { return "Station ID [" + this.id + "]"; }
}

