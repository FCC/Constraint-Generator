------------------------------------------------------------------------------------------------------------
----AUTHOR:  FEDERAL COMMUNICATIONS COMMISSION (FCC)                                                    ---- 
----LAST MODIFIED:  OCTOBER 31, 2014                                                                    ----
------------------------------------------------------------------------------------------------------------

---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
--------- ******* PROCESSING OF CHANNELS 2-4 ******* ---------
---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
-------------------------------------------------------------------------------------------------------------------------
--------STEP 1:  CREATE SINGLE TABLE THAT IDENTIFIES SERVICE POINTS OF ALL STATIONS REPLICATED ONTO CHANNELS 2-4--------- 
-------------------------------------------------------------------------------------------------------------------------

--->>>>> FOR US STATIONS, CREATES STAGING TABLE OF SERVICE POINTS BASED ON BASELINE POINTS <<<<<---
drop table if exists ia.tvsoftware_analysis_orig_rep_2_4; commit; 
create table ia.tvsoftware_analysis_orig_rep_2_4
(
orig_facilityid integer,
pointkey integer,
population numeric, 
orig_channel integer,
orig_serviceflag integer,
interference_free_flag integer,
ch_2 integer,
ch_2_serviceflag integer,
ch_3 integer,
ch_3_serviceflag integer,
ch_4 integer,
ch_4_serviceflag integer
); commit;

insert into ia.tvsoftware_analysis_orig_rep_2_4

select a.orig_facilityid, a.pointkey, population, orig_channel, orig_serviceflag, interference_free_flag,
b.channel as ch_2, b.serviceflag as ch_2_serviceflag,
c.channel as ch_3, c.serviceflag as ch_3_serviceflag, 
d.channel as ch_4, d.serviceflag as ch_4_serviceflag
from
(       /* RETRIEVES POINTS OF COVERAGE INFORMATION FOR EACH STATION */
	select orig_facilityid, pointkey, population, orig_channel, orig_serviceflag, interference_free_flag
	from ia.tvsoftware_rep_station_points
) a
left join
(       /* RETRIEVES POINTS OF COVERAGE FOR STATIONS REPLICATED ONTO CHANNEL 2 */
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_2_4
	where channel = 2 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) b
on a.orig_facilityid = b.facilityid and a.pointkey = b.pointkey
left join
(       /* RETRIEVES POINTS OF COVERAGE FOR STATIONS REPLICATED ONTO CHANNEL 3 */
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_2_4
	where channel = 3 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) c
on a.orig_facilityid = c.facilityid and a.pointkey = c.pointkey
left join
(       /* RETRIEVES POINTS OF COVERAGE FOR STATIONS REPLICATED ONTO CHANNEL 4 */
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_2_4
	where channel = 4 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) d
on a.orig_facilityid = d.facilityid and a.pointkey = d.pointkey; commit; 


create index tvsoftware_analysis_orig_rep_2_4_orig_facilityid on ia.tvsoftware_analysis_orig_rep_2_4 (orig_facilityid); commit; 
create index tvsoftware_analysis_orig_rep_2_4_pointkey on ia.tvsoftware_analysis_orig_rep_2_4 (pointkey); commit; 
create index tvsoftware_analysis_orig_rep_2_4_orig_channel on ia.tvsoftware_analysis_orig_rep_2_4 (orig_channel); commit; 
create index tvsoftware_analysis_orig_rep_2_4_ch_2 on ia.tvsoftware_analysis_orig_rep_2_4 (ch_2); commit; 
create index tvsoftware_analysis_orig_rep_2_4_ch_3 on ia.tvsoftware_analysis_orig_rep_2_4 (ch_3); commit; 
create index tvsoftware_analysis_orig_rep_2_4_ch_4 on ia.tvsoftware_analysis_orig_rep_2_4 (ch_4); commit; 
create index tvsoftware_analysis_orig_rep_2_4_interference_free_flag on ia.tvsoftware_analysis_orig_rep_2_4 (interference_free_flag); commit;


--->>>>> FOR CANADIAN STATIONS, CREATES STAGING TABLE OF SERVICE POINTS BASED ON BASELINE POINTS <<<<<---
drop table if exists ia.tvsoftware_analysis_canada_rep_2_4; commit; 
create table ia.tvsoftware_analysis_canada_rep_2_4
(
orig_facilityid integer,
pointkey integer,
population numeric, 
orig_channel integer,
orig_serviceflag integer,
interference_free_flag integer,
ch_2 integer,
ch_2_serviceflag integer,
ch_3 integer,
ch_3_serviceflag integer,
ch_4 integer,
ch_4_serviceflag integer
); commit;

insert into ia.tvsoftware_analysis_canada_rep_2_4

select a.orig_facilityid, a.pointkey, population, orig_channel, orig_serviceflag, interference_free_flag,
b.channel as ch_2, b.serviceflag as ch_2_serviceflag,
c.channel as ch_3, c.serviceflag as ch_3_serviceflag, 
d.channel as ch_4, d.serviceflag as ch_4_serviceflag
from
(       /* RETRIEVES POINTS OF COVERAGE INFORMATION FOR EACH STATION */
	select orig_facilityid, pointkey, population, orig_channel, orig_serviceflag, interference_free_flag
	from ia.tvsoftware_rep_station_points_canada
) a
left join
(       /* RETRIEVES POINTS OF COVERAGE FOR STATIONS REPLICATED ONTO CHANNEL 2 */
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_2_4
	where channel = 2 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA')
) b
on a.orig_facilityid = b.facilityid and a.pointkey = b.pointkey
left join
(       /* RETRIEVES POINTS OF COVERAGE FOR STATIONS REPLICATED ONTO CHANNEL 3 */
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_2_4
	where channel = 3 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) c
on a.orig_facilityid = c.facilityid and a.pointkey = c.pointkey
left join
(       /* RETRIEVES POINTS OF COVERAGE FOR STATIONS REPLICATED ONTO CHANNEL 4 */
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_2_4
	where channel = 4 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) d
on a.orig_facilityid = d.facilityid and a.pointkey = d.pointkey; commit; 


create index tvsoftware_analysis_canada_rep_2_4_orig_facilityid on ia.tvsoftware_analysis_canada_rep_2_4 (orig_facilityid); commit; 
create index tvsoftware_analysis_canada_rep_2_4_pointkey on ia.tvsoftware_analysis_canada_rep_2_4 (pointkey); commit; 
create index tvsoftware_analysis_canada_rep_2_4_orig_channel on ia.tvsoftware_analysis_canada_rep_2_4 (orig_channel); commit; 
create index tvsoftware_analysis_canada_rep_2_4_ch_2 on ia.tvsoftware_analysis_canada_rep_2_4 (ch_2); commit; 
create index tvsoftware_analysis_canada_rep_2_4_ch_3 on ia.tvsoftware_analysis_canada_rep_2_4 (ch_3); commit; 
create index tvsoftware_analysis_canada_rep_2_4_ch_4 on ia.tvsoftware_analysis_canada_rep_2_4 (ch_4); commit; 
create index tvsoftware_analysis_canada_rep_2_4_interference_free_flag on ia.tvsoftware_analysis_canada_rep_2_4 (interference_free_flag); commit;


------------------------------------------------------------------------------------------------------
--------STEP 2:  CALCULATE PAIRWISE INTERFERENCE PERCENTAGES FOR ALL STATIONS ON CHANNELS 2-4--------- 
------------------------------------------------------------------------------------------------------
drop table if exists ia.tvsoftware_pairwise_result_2_4; commit; 
create table ia.tvsoftware_pairwise_result_2_4 
(
orig_facilityid integer,
orig_channel integer,
interferingfacilityid integer,
interferingchannel integer,
interference_population numeric,
interference_free_pop numeric,
interference_pct numeric
); commit;

------>>>>>> US STATIONS - CHANNEL 2

insert into ia.tvsoftware_pairwise_result_2_4

select y.orig_facilityid, ch_2 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(       /* FIND THE TOTAL INTERFERENCE (IN TERMS OF POPULATION) CAUSED BY STATIONS ON CHANNEL 2 OR AN ADJACENT CHANNEL, GIVEN THAT THE STUDY STATION IS ASSIGNED TO CHANNEL 2 */
	select orig_facilityid, ch_2, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_2_4 a, ia.tvsoftware_interference_2_4 b
        where a.orig_facilityid = b.facilityid and a.ch_2 = b.channel and a.pointkey = b.pointkey and b.channel = 2 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_2, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(       /* FIND THE INTERFERENCE FREE POPULATION OF EACH STATION */
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_2_4
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 



------>>>>>> US STATIONS - CHANNEL 3

insert into ia.tvsoftware_pairwise_result_2_4

select y.orig_facilityid, ch_3 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(       /* FIND THE TOTAL INTERFERENCE (IN TERMS OF POPULATION) CAUSED BY STATIONS ON CHANNEL 3 OR AN ADJACENT CHANNEL, GIVEN THAT THE STUDY STATION IS ASSIGNED TO CHANNEL 3 */
	select orig_facilityid, ch_3, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_2_4 a, ia.tvsoftware_interference_2_4 b
        where a.orig_facilityid = b.facilityid and a.ch_3 = b.channel and a.pointkey = b.pointkey and b.channel = 3 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_3, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(       /* FIND THE INTERFERENCE FREE POPULATION OF EACH STATION */
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_2_4
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 



------>>>>>> US STATIONS - CHANNEL 4

insert into ia.tvsoftware_pairwise_result_2_4

select y.orig_facilityid, ch_4 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(       /* FIND THE TOTAL INTERFERENCE (IN TERMS OF POPULATION) CAUSED BY STATIONS ON CHANNEL 4 OR AN ADJACENT CHANNEL, GIVEN THAT THE STUDY STATION IS ASSIGNED TO CHANNEL 4 */
	select orig_facilityid, ch_4, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_2_4 a, ia.tvsoftware_interference_2_4 b
        where a.orig_facilityid = b.facilityid and a.ch_4 = b.channel and a.pointkey = b.pointkey and b.channel = 4 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_4, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(       /* FIND THE INTERFERENCE FREE POPULATION OF EACH STATION */
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_2_4
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 



------>>>>>> CANADIAN STATIONS - CHANNEL 2

insert into ia.tvsoftware_pairwise_result_2_4

select y.orig_facilityid, ch_2 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(              /* FIND THE TOTAL INTERFERENCE (IN TERMS OF POPULATION) CAUSED BY STATIONS ON CHANNEL 2 OR AN ADJACENT CHANNEL, GIVEN THAT THE STUDY STATION IS ASSIGNED TO CHANNEL 2 */
	select orig_facilityid, ch_2, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_2_4 a, ia.tvsoftware_interference_2_4 b
        where a.orig_facilityid = b.facilityid and a.ch_2 = b.channel and a.pointkey = b.pointkey and b.channel = 2 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_2, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_2_4
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 



------>>>>>> CANADIAN STATIONS - CHANNEL 3

insert into ia.tvsoftware_pairwise_result_2_4

select y.orig_facilityid, ch_3 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(       /* FIND THE TOTAL INTERFERENCE (IN TERMS OF POPULATION) CAUSED BY STATIONS ON CHANNEL 3 OR AN ADJACENT CHANNEL, GIVEN THAT THE STUDY STATION IS ASSIGNED TO CHANNEL 3 */
	select orig_facilityid, ch_3, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_2_4 a, ia.tvsoftware_interference_2_4 b
        where a.orig_facilityid = b.facilityid and a.ch_3 = b.channel and a.pointkey = b.pointkey and b.channel = 3 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_3, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_2_4
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 



------>>>>>> CANADIAN STATIONS - CHANNEL 4

insert into ia.tvsoftware_pairwise_result_2_4

select y.orig_facilityid, ch_4 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(       /* FIND THE TOTAL INTERFERENCE (IN TERMS OF POPULATION) CAUSED BY STATIONS ON CHANNEL 4 OR AN ADJACENT CHANNEL, GIVEN THAT THE STUDY STATION IS ASSIGNED TO CHANNEL 4 */
	select orig_facilityid, ch_4, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_2_4 a, ia.tvsoftware_interference_2_4 b
        where a.orig_facilityid = b.facilityid and a.ch_4 = b.channel and a.pointkey = b.pointkey and b.channel = 4 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_4, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_2_4
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 
