package com.computechinc.tvmodel.util;

import java.util.Comparator;

import com.computechinc.tvmodel.containers.*;

public class Comparators {
	public static final Comparator<Station> StationByAscFacilityId = new Comparator<Station>() {
		public int compare(Station a, Station b) {
			return ((a.getId() < b.getId()) ? -1 : (a.getId() == b.getId()) ? 0 : 1);
		}
	};
}

