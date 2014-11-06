package com.computechinc.tvmodel.containers;

import com.computechinc.tvmodel.ConstrsGenrDriver;

import java.util.*;

public final class Channel {
	//========================================================================//
	// Class level
	//========================================================================//
	public static HashSet<Long> forbiddenChannelNumbers = new HashSet<Long>();
	public static String BelowVHF = "BelowVHF";
	public static String LowerVHF = "LowerVHF";// from 2-6
	public static String UpperVHF = "UpperVHF";// from 7-13
	public static String UHF = "UHF";// from 14-51
	public static String AboveUHF = "AboveUHF";
	public static int minTvChannelNum = 2;
	public static int maxTvChannelNum = 51;
	private static HashMap<Long,Channel> channelNum2Channel = new HashMap<Long,Channel>();
	public static ArrayList<Long> adjLevels = new ArrayList<Long>();
	static {
		adjLevels.add(new Long(0));
		//
		adjLevels.add(new Long(-1));
		adjLevels.add(new Long(-2));
		adjLevels.add(new Long(-3));
		adjLevels.add(new Long(-4));
		adjLevels.add(new Long(-7));
		adjLevels.add(new Long(-8));
		adjLevels.add(new Long(-14));
		adjLevels.add(new Long(-15));
		//
		adjLevels.add(new Long(1));
		adjLevels.add(new Long(2));
		adjLevels.add(new Long(3));
		adjLevels.add(new Long(4));
		adjLevels.add(new Long(7));
		adjLevels.add(new Long(8));
		adjLevels.add(new Long(14));
		adjLevels.add(new Long(15));
	}
	public static Channel acquireChannel(long channelNum) throws Exception {
		Channel newChannel = findChannel(new Long(channelNum));
		if (newChannel == null) {
			String channelBand = getBand(channelNum);
			newChannel = new Channel(channelNum, channelBand);
			channelNum2Channel.put(channelNum, newChannel);
		}
		return newChannel;
	}
	public static Channel findChannel(long channelNum) { return channelNum2Channel.get(channelNum); }
	public static String getBand(Channel channel) throws Exception { return getBand(channel.getChannelNum()); }
	public static String getBand(long channelNum) throws Exception {
		if (channelNum < 2) {
			return Channel.BelowVHF;
		}
		else if (2 <= channelNum && channelNum <= 6) {
			return Channel.LowerVHF;
		}
		else if (7 <= channelNum && channelNum <= 13) {
			return Channel.UpperVHF;
		}
		else if (14 <= channelNum && channelNum <= 51) {
			return Channel.UHF;
		}
		if (51 < channelNum) {
			return Channel.AboveUHF;
		}
		else {
			ConstrsGenrDriver.displayErr("Error: channel number [" + channelNum + "] is beyond the scope of this application.");
			throw new Exception("Error: channel number [" + channelNum + "] is beyond the scope of this application.");
		}
	}
	public static Channel getChannel(long channelNum) { return channelNum2Channel.get(channelNum); }
	public static HashMap<Long,Channel> getChannelNum2Channel() { return new HashMap<Long,Channel>(channelNum2Channel); }
	public static Channel getHighestChannel(String subBand) throws Exception {
		if (subBand.equalsIgnoreCase(Channel.LowerVHF)) {
			return Channel.getChannel(6);
		}
		else if (subBand.equalsIgnoreCase(Channel.UpperVHF)) {
			return Channel.getChannel(13);
		}
		else if (subBand.equalsIgnoreCase(Channel.UHF)) {
			return Channel.getChannel(51);
		}
		else {
			ConstrsGenrDriver.displayErr("Error: Unknown sub band [" + subBand + "].");
			throw new Exception("Error: Unknown sub band [" + subBand + "].");
		}
	}
	public static ArrayList<Channel> getLowerVHFChannels() throws Exception {
		ArrayList<Channel> lowerVHFChannels = new ArrayList<Channel>();
		for (long channelNum = 2; channelNum <= 6; ++channelNum) {
			if (forbiddenChannelNumbers.contains(channelNum) == false) {
				Channel channel = acquireChannel(channelNum);
				lowerVHFChannels.add(channel);
			}
		}
		//
		return lowerVHFChannels;
	}
	public static Channel getLowestChannel(String subBand) throws Exception {
		if (subBand.equalsIgnoreCase(Channel.LowerVHF)) {
			return Channel.getChannel(2);
		}
		else if (subBand.equalsIgnoreCase(Channel.UpperVHF)) {
			return Channel.getChannel(7);
		}
		else if (subBand.equalsIgnoreCase(Channel.UHF)) {
			return Channel.getChannel(14);
		}
		else {
			ConstrsGenrDriver.displayErr("Error: Unknown sub band [" + subBand + "].");
			throw new Exception("Error: Unknown sub band [" + subBand + "].");
		}
	}
	public static ArrayList<Channel> getSubBandChannels(String subBand) throws Exception {
		if (subBand.equalsIgnoreCase(Profile.StringParamValue.All)) {
			return getTVChannels();
		}
		else if (subBand.equalsIgnoreCase(Profile.StringParamValue.VHF)) {
			return getVHFChannels();
		}
		else if (subBand.equalsIgnoreCase(Profile.StringParamValue.LowerVHF)) {
			return getLowerVHFChannels();
		}
		else if (subBand.equalsIgnoreCase(Profile.StringParamValue.UpperVHF)) {
			return getUpperVHFChannels();
		}
		else if (subBand.equalsIgnoreCase(Profile.StringParamValue.UHF)) {
			return getUHFChannels();
		}
		else {
			ConstrsGenrDriver.displayErr("Error: Unknown sub band [" + subBand + "].");
			throw new Exception("Error: Unknown sub banforbiddenChannelNumbersd [" + subBand + "].");
		}
	}
	public static ArrayList<Channel> getTVChannels() throws Exception {
		ArrayList<Channel> tvChannels = new ArrayList<Channel>();
		for (long channelNum = minTvChannelNum; channelNum <= maxTvChannelNum; ++channelNum) {
			if (forbiddenChannelNumbers.contains(channelNum) == false) {
				Channel channel = acquireChannel(channelNum);
				tvChannels.add(channel);
			}
		}
		//
		return tvChannels;
	}
	public static ArrayList<Channel> getUHFChannels() throws Exception {
		ArrayList<Channel> uhfChannels = new ArrayList<Channel>();
		for (long channelNum = 14; channelNum <= 51; ++channelNum) {
			if (forbiddenChannelNumbers.contains(channelNum) == false) {
				Channel channel = acquireChannel(channelNum);
				uhfChannels.add(channel);
			}
		}
		//
		return uhfChannels;
	}
	public static ArrayList<Channel> getUpperVHFChannels() throws Exception {
		ArrayList<Channel> upperVHFChannels = new ArrayList<Channel>();
		for (long channelNum = 7; channelNum <= 13; ++channelNum) {
			if (forbiddenChannelNumbers.contains(channelNum) == false) {
				Channel channel = acquireChannel(channelNum);
				upperVHFChannels.add(channel);
			}
		}
		//
		return upperVHFChannels;
	}
	public static ArrayList<Channel> getVHFChannels() throws Exception {
		ArrayList<Channel> vhfChannels = new ArrayList<Channel>();
		for (long channelNum = 2; channelNum <= 13; ++channelNum) {
			if (forbiddenChannelNumbers.contains(channelNum) == false) {
				Channel channel = acquireChannel(channelNum);
				vhfChannels.add(channel);
			}
		}
		//
		return vhfChannels;
	}
	public static void setAdjChannels2ConsideredChannels() throws Exception {
		ArrayList<Long> channelNumbers = new ArrayList<Long>(channelNum2Channel.keySet());
		Collections.sort(channelNumbers);
		for (Long channelNum : channelNumbers) {
			Channel channel = channelNum2Channel.get(channelNum);
			// bottom end channel
			if (channelNum == 2 || channelNum == 7 || channelNum == 14) {
				long adjAboveChannelNum = channelNum + 1;
				Channel adjChannelAbove = channelNum2Channel.get(adjAboveChannelNum);
				if (adjChannelAbove == null) {
					ConstrsGenrDriver.displayErr("Error: Channel number [" + adjAboveChannelNum + "] is not available.");
					throw new Exception("Error: Channel number [" + adjAboveChannelNum + "] is not available.");
				}
				channel.setAdjChannelBelow(null);
				channel.setAdjChannelAbove(adjChannelAbove);
			}
			// top end channel, from channel 51 we want to reach channel 52 in case we have protected station in that channel
			else if (channelNum == 6 || channelNum == 13 || channelNum == 51) {
				long adjBelowChannelNum = channelNum - 1;
				Channel adjChannelBelow = channelNum2Channel.get(adjBelowChannelNum);
				if (adjChannelBelow == null) {
					ConstrsGenrDriver.displayErr("Error: Channel number [" + adjBelowChannelNum + "] is not available.");
					throw new Exception("Error: Channel number [" + adjBelowChannelNum + "] is not available.");
				}
				channel.setAdjChannelBelow(adjChannelBelow);
				channel.setAdjChannelAbove(null);
			}
			// middle channel
			else if ((3 <= channelNum && channelNum <= 5) || (8 <= channelNum && channelNum <= 12) || (15 <= channelNum && channelNum <= 50)) {
				long adjBelowChannelNum = channelNum - 1;
				Channel adjChannelBelow = channelNum2Channel.get(adjBelowChannelNum);
				if (adjChannelBelow == null) {
					ConstrsGenrDriver.displayErr("Error: Channel number [" + adjBelowChannelNum + "] is not available.");
					throw new Exception("Error: Channel number [" + adjBelowChannelNum + "] is not available.");
				}
				long adjAboveChannelNum = channelNum + 1;
				Channel adjChannelAbove = channelNum2Channel.get(adjAboveChannelNum);
				if (adjChannelAbove == null) {
					ConstrsGenrDriver.displayErr("Error: Channel number [" + adjAboveChannelNum + "] is not available.");
					throw new Exception("Error: Channel number [" + adjAboveChannelNum + "] is not available.");
				}
				channel.setAdjChannelBelow(adjChannelBelow);
				channel.setAdjChannelAbove(adjChannelAbove);
			}
			else {
				ConstrsGenrDriver.displayLog("Warning: Channel number [" + channelNum + "] is not considered.");
			}
		}
		// 
		for (Channel channel : Channel.getUHFChannels()) {
			Channel.setAdjLevelChannels(channel);
		}
	}
	private static void setAdjLevelChannels(Channel channel) throws Exception {
		if (channel.getChannelBand().equalsIgnoreCase(Channel.UHF)) {
			channel.setAdjLevelChannel(new Long(0), channel);
			//
			Channel currAboveChannel = channel;
			Channel currBelowChannel = channel;
			Channel adjChannelAbove = null;
			Channel adjChannelBelow = null;
			//
			for (long i = 1; i <= 15; ++i) {
				Long level = new Long(i);
				if (currAboveChannel != null) {
					adjChannelAbove = currAboveChannel.getAdjChannelAbove();
					if (adjChannelAbove == null && currAboveChannel.getChannelNum() < 51) {
						ConstrsGenrDriver.displayErr("Error: Inconsistent adjacent channel link list");
						throw new Exception("Error: Inconsistent adjacent channel link list");
					}
				}
				if (currBelowChannel != null) {
					adjChannelBelow = currBelowChannel.getAdjChannelBelow();
					if (adjChannelBelow == null && currBelowChannel.getChannelNum() > 14) {
						ConstrsGenrDriver.displayErr("Error: Inconsistent adjacent channel link list");
						throw new Exception("Error: Inconsistent adjacent channel link list");
					}
				}
				if (Channel.adjLevels.contains(level)) {
					channel.setAdjLevelChannel(level, adjChannelAbove);
					channel.setAdjLevelChannel(-level, adjChannelBelow);
				}
				currAboveChannel = adjChannelAbove;
				currBelowChannel = adjChannelBelow;
			}
		}
		else {
			ConstrsGenrDriver.displayErr("Error: " + channel + " out of range; Adj Level beyond +/-1 is only applied to UHF analog Mexico stations");
			throw new Exception("Error: " + channel + " out of range; Adj Level beyond +/-1 is only applied to UHF analog Mexico stations");
		}
	}
	
	//========================================================================//
	// Instance level
	//========================================================================//
	private long channelNum;
	private String channelBand;// "lower-VHF", "upper-VHF", and "UHF"
	private Channel adjChannelAbove = null;
	private Channel adjChannelBelow = null;
	private HashMap<Long, Channel> adjLevel2Channel = null;
	//constructors
	private Channel(long channelNum, String channelBand) {
		this.channelNum = channelNum;
		this.channelBand = channelBand;
		adjLevel2Channel = new HashMap<Long, Channel>();
	}
	//
	// methods
	
	public boolean equals(Channel aChannel) { return this.channelNum == aChannel.getChannelNum(); }
	public Channel getAdjChannel(Long level) { return this.adjLevel2Channel.get(level); }
	public Channel getAdjChannelAbove() { return adjChannelAbove; }
	public Channel getAdjChannelBelow() { return adjChannelBelow; }
	public ArrayList<Channel> getAdjChannels() { 
		ArrayList<Channel> adjChannels = new ArrayList<Channel>();
		if (this.adjChannelAbove != null) {
			adjChannels.add(this.adjChannelAbove);
		}
		if (this.adjChannelBelow != null) {
			adjChannels.add(this.adjChannelBelow);
		}
		return adjChannels;
	}
	public String getChannelBand() { return channelBand; }
	public long getChannelNum() { return channelNum; }
	public void setAdjChannelAbove(Channel adjChannelAbove) { this.adjChannelAbove = adjChannelAbove; }
	public void setAdjChannelBelow(Channel adjChannelBelow) { this.adjChannelBelow = adjChannelBelow; }
	public void setAdjLevelChannel(Long level, Channel adjChannel) throws Exception {
		if (adjLevel2Channel.get(level) == null) {
			adjLevel2Channel.put(level, adjChannel);
		}
		else {
			ConstrsGenrDriver.displayErr("Error: Duplicate insertion, at level [" + level +  "] existing channel is [" + adjLevel2Channel.get(level) + "], while the new channel [" + adjChannel + "]");
			throw new Exception("Error: Duplicate insertion, at level [" + level +  "] existing channel is [" + adjLevel2Channel.get(level) + "], while the new channel [" + adjChannel + "]");
		}
	}
	public void setChannelBand(String channelBand) { this.channelBand = channelBand; }
	@Override
	public String toString() { return "Channel number [" + this.channelNum + "]"; }
}

