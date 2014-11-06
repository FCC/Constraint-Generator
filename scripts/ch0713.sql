------------------------------------------------------------------------------------------------------------
----AUTHOR:  FEDERAL COMMUNICATIONS COMMISSION (FCC)                                                    ---- 
----LAST MODIFIED:  OCTOBER 31, 2014                                                                    ----
------------------------------------------------------------------------------------------------------------

---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
--------- ******* PROCESSING OF CHANNELS 7-13 ******* ---------
---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
---------------------------------------------------------------------------------------------------
-------STEP 1:  LOAD SERVICE AND INTERFERENCE POINT DATA FROM CHANNEL 7-13 DATA FILES-------
---------------------------------------------------------------------------------------------------

-----THIS CREATES TABLE INTO WHICH WE LOAD THE SERVICE.CSV FILE FROM THE CHANNEL 7-13 RUN-----
drop table if exists ia.tvsoftware_service_7_13; commit; 
create table ia.tvsoftware_service_7_13
(
pointkey integer,
facilityid integer,
channel integer,
serviceFlag integer
); commit;


-----THIS CREATES TABLE INTO WHICH WE LOAD THE FIRST INTERFERENCE.CSV FILE-----
drop table if exists ia.tvsoftware_interference_7_13; commit; 
create table ia.tvsoftware_interference_7_13
(
pointKey integer,
facilityID integer,
channel integer,
interferingFacilityID integer,
interferingChannel integer
); commit;

--->>>>> COPIES SERVICE.CSV INTO TABLE <<<<<---
\copy ia.tvsoftware_service_7_13 from 'tvstudyprocessing/ch0713/service.csv' delimiter ',' csv; commit; 

--->>>>> COPIES INTERFERENCE.CSV INTO TABLE <<<<<---
\copy ia.tvsoftware_interference_7_13 from 'tvstudyprocessing/ch0713/interference.csv' delimiter ',' csv; commit; 

--->>>>> CREATES INDEXES ON FREQUENTLY USED COLUMNS <<<<<---
create index tvsoftware_service_7_13_pointkey on ia.tvsoftware_service_7_13 (pointkey); commit; 
create index tvsoftware_service_7_13_channel on ia.tvsoftware_service_7_13 (channel); commit; 
create index tvsoftware_service_7_13_facilityid on ia.tvsoftware_service_7_13 (facilityid); commit; 
create index tvsoftware_interference_7_13_pointkey on ia.tvsoftware_interference_7_13 (pointkey); commit; 
create index tvsoftware_interference_7_13_channel on ia.tvsoftware_interference_7_13 (channel); commit; 
create index tvsoftware_interference_7_13_facilityid on ia.tvsoftware_interference_7_13 (facilityid); commit;


-----------------------------------------------------------------------------------------------------------------------------
--------STEP 2:  CREATE SINGLE TABLE THAT IDENTIFIES SERVICE POINTS OF ALL STATIONS REPLICATED ONTO CHANNELS 7 - 13 --------- 
-----------------------------------------------------------------------------------------------------------------------------

--->>>>> FOR US STATIONS, CREATES STAGING TABLE OF SERVICE POINTS BASED ON BASELINE POINTS <<<<<---
drop table if exists ia.tvsoftware_analysis_orig_rep_7_13; commit; 
create table ia.tvsoftware_analysis_orig_rep_7_13
(
orig_facilityid integer,
pointkey integer,
population numeric, 
orig_channel integer,
orig_serviceflag integer,
interference_free_flag integer,
ch_7 integer,
ch_7_serviceflag integer,
ch_8 integer,
ch_8_serviceflag integer, 
ch_9 integer,
ch_9_serviceflag integer,
ch_10 integer,
ch_10_serviceflag integer, 
ch_11 integer,
ch_11_serviceflag integer,
ch_12 integer,
ch_12_serviceflag integer, 
ch_13 integer,
ch_13_serviceflag integer
); commit;


insert into ia.tvsoftware_analysis_orig_rep_7_13

select a.orig_facilityid, a.pointkey, population, orig_channel, orig_serviceflag, interference_free_flag,
b.channel as ch_7, b.serviceflag as ch_7_serviceflag,
c.channel as ch_8, c.serviceflag as ch_8_serviceflag, 
d.channel as ch_9, d.serviceflag as ch_9_serviceflag,
e.channel as ch_10, e.serviceflag as ch_10_serviceflag,
f.channel as ch_11, f.serviceflag as ch_11_serviceflag,
g.channel as ch_12, g.serviceflag as ch_12_serviceflag,
h.channel as ch_13, h.serviceflag as ch_13_serviceflag
from
(       
	select orig_facilityid, pointkey, population, orig_channel, orig_serviceflag, interference_free_flag
	from ia.tvsoftware_rep_station_points
) a
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_7_13
	where channel = 7 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) b
on a.orig_facilityid = b.facilityid and a.pointkey = b.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_7_13
	where channel = 8 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) c
on a.orig_facilityid = c.facilityid and a.pointkey = c.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_7_13
	where channel = 9 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) d
on a.orig_facilityid = d.facilityid and a.pointkey = d.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_7_13
	where channel = 10 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) e
on a.orig_facilityid = e.facilityid and a.pointkey = e.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_7_13
	where channel = 11 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) f
on a.orig_facilityid = f.facilityid and a.pointkey = f.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_7_13
	where channel = 12 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) g
on a.orig_facilityid = g.facilityid and a.pointkey = g.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_7_13
	where channel = 13 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) h
on a.orig_facilityid = h.facilityid and a.pointkey = h.pointkey; commit; 


create index tvsoftware_analysis_orig_rep_7_13_orig_facilityid on ia.tvsoftware_analysis_orig_rep_7_13 (orig_facilityid); commit; 
create index tvsoftware_analysis_orig_rep_7_13_pointkey on ia.tvsoftware_analysis_orig_rep_7_13 (pointkey); commit; 
create index tvsoftware_analysis_orig_rep_7_13_orig_channel on ia.tvsoftware_analysis_orig_rep_7_13 (orig_channel); commit; 
create index tvsoftware_analysis_orig_rep_7_13_ch_7 on ia.tvsoftware_analysis_orig_rep_7_13 (ch_7); commit; 
create index tvsoftware_analysis_orig_rep_7_13_ch_8 on ia.tvsoftware_analysis_orig_rep_7_13 (ch_8); commit; 
create index tvsoftware_analysis_orig_rep_7_13_ch_9 on ia.tvsoftware_analysis_orig_rep_7_13 (ch_9); commit; 
create index tvsoftware_analysis_orig_rep_7_13_ch_10 on ia.tvsoftware_analysis_orig_rep_7_13 (ch_10); commit; 
create index tvsoftware_analysis_orig_rep_7_13_ch_11 on ia.tvsoftware_analysis_orig_rep_7_13 (ch_11); commit; 
create index tvsoftware_analysis_orig_rep_7_13_ch_12 on ia.tvsoftware_analysis_orig_rep_7_13 (ch_12); commit; 
create index tvsoftware_analysis_orig_rep_7_13_ch_13 on ia.tvsoftware_analysis_orig_rep_7_13 (ch_13); commit; 
create index tvsoftware_analysis_orig_rep_7_13_interference_free_flag on ia.tvsoftware_analysis_orig_rep_7_13 (interference_free_flag); commit;


--->>>>> FOR CANADIAN STATIONS, CREATES STAGING TABLE OF SERVICE POINTS BASED ON BASELINE POINTS <<<<<---
drop table if exists ia.tvsoftware_analysis_canada_rep_7_13; commit; 
create table ia.tvsoftware_analysis_canada_rep_7_13
(
orig_facilityid integer,
pointkey integer,
population numeric, 
orig_channel integer,
orig_serviceflag integer,
interference_free_flag integer,
ch_7 integer,
ch_7_serviceflag integer,
ch_8 integer,
ch_8_serviceflag integer, 
ch_9 integer,
ch_9_serviceflag integer,
ch_10 integer,
ch_10_serviceflag integer, 
ch_11 integer,
ch_11_serviceflag integer,
ch_12 integer,
ch_12_serviceflag integer, 
ch_13 integer,
ch_13_serviceflag integer
); commit;


insert into ia.tvsoftware_analysis_canada_rep_7_13

select a.orig_facilityid, a.pointkey, population, orig_channel, orig_serviceflag, interference_free_flag,
b.channel as ch_7, b.serviceflag as ch_7_serviceflag,
c.channel as ch_8, c.serviceflag as ch_8_serviceflag, 
d.channel as ch_9, d.serviceflag as ch_9_serviceflag,
e.channel as ch_10, e.serviceflag as ch_10_serviceflag,
f.channel as ch_11, f.serviceflag as ch_11_serviceflag,
g.channel as ch_12, g.serviceflag as ch_12_serviceflag,
h.channel as ch_13, h.serviceflag as ch_13_serviceflag
from
(       
	select orig_facilityid, pointkey, population, orig_channel, orig_serviceflag, interference_free_flag
	from ia.tvsoftware_rep_station_points_canada
) a
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_7_13
	where channel = 7 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) b
on a.orig_facilityid = b.facilityid and a.pointkey = b.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_7_13
	where channel = 8 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) c
on a.orig_facilityid = c.facilityid and a.pointkey = c.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_7_13
	where channel = 9 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) d
on a.orig_facilityid = d.facilityid and a.pointkey = d.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_7_13
	where channel = 10 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) e
on a.orig_facilityid = e.facilityid and a.pointkey = e.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_7_13
	where channel = 11 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) f
on a.orig_facilityid = f.facilityid and a.pointkey = f.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_7_13
	where channel = 12 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) g
on a.orig_facilityid = g.facilityid and a.pointkey = g.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_7_13
	where channel = 13 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) h
on a.orig_facilityid = h.facilityid and a.pointkey = h.pointkey; commit; 


create index tvsoftware_analysis_canada_rep_7_13_orig_facilityid on ia.tvsoftware_analysis_canada_rep_7_13 (orig_facilityid); commit; 
create index tvsoftware_analysis_canada_rep_7_13_pointkey on ia.tvsoftware_analysis_canada_rep_7_13 (pointkey); commit; 
create index tvsoftware_analysis_canada_rep_7_13_orig_channel on ia.tvsoftware_analysis_canada_rep_7_13 (orig_channel); commit; 
create index tvsoftware_analysis_canada_rep_7_13_ch_7 on ia.tvsoftware_analysis_canada_rep_7_13 (ch_7); commit; 
create index tvsoftware_analysis_canada_rep_7_13_ch_8 on ia.tvsoftware_analysis_canada_rep_7_13 (ch_8); commit; 
create index tvsoftware_analysis_canada_rep_7_13_ch_9 on ia.tvsoftware_analysis_canada_rep_7_13 (ch_9); commit; 
create index tvsoftware_analysis_canada_rep_7_13_ch_10 on ia.tvsoftware_analysis_canada_rep_7_13 (ch_10); commit; 
create index tvsoftware_analysis_canada_rep_7_13_ch_11 on ia.tvsoftware_analysis_canada_rep_7_13 (ch_11); commit; 
create index tvsoftware_analysis_canada_rep_7_13_ch_12 on ia.tvsoftware_analysis_canada_rep_7_13 (ch_12); commit; 
create index tvsoftware_analysis_canada_rep_7_13_ch_13 on ia.tvsoftware_analysis_canada_rep_7_13 (ch_13); commit; 
create index tvsoftware_analysis_canada_rep_7_13_interference_free_flag on ia.tvsoftware_analysis_canada_rep_7_13 (interference_free_flag); commit;


-------------------------------------------------------------------------------------------------------
--------STEP 3:  CALCULATE PAIRWISE INTERFERENCE PERCENTAGES FOR ALL STATIONS ON CHANNELS 7-13--------- 
-------------------------------------------------------------------------------------------------------
drop table if exists ia.tvsoftware_pairwise_result_7_13; commit; 
create table ia.tvsoftware_pairwise_result_7_13
(
orig_facilityid integer,
orig_channel integer,
interferingfacilityid integer,
interferingchannel integer,
interference_population numeric,
interference_free_pop numeric,
interference_pct numeric
); commit;

------>>>>>> US STATIONS - CHANNEL 7

insert into ia.tvsoftware_pairwise_result_7_13

select y.orig_facilityid, ch_7 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_7, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_7_13 a, ia.tvsoftware_interference_7_13 b
        where a.orig_facilityid = b.facilityid and a.ch_7 = b.channel and a.pointkey = b.pointkey and b.channel = 7 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_7, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_7_13
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;


------>>>>>> US STATIONS - CHANNEL 8

insert into ia.tvsoftware_pairwise_result_7_13

select y.orig_facilityid, ch_8 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_8, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_7_13 a, ia.tvsoftware_interference_7_13 b
        where a.orig_facilityid = b.facilityid and a.ch_8 = b.channel and a.pointkey = b.pointkey and b.channel = 8 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_8, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_7_13
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> US STATIONS - CHANNEL 9

insert into ia.tvsoftware_pairwise_result_7_13

select y.orig_facilityid, ch_9 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_9, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_7_13 a, ia.tvsoftware_interference_7_13 b
        where a.orig_facilityid = b.facilityid and a.ch_9 = b.channel and a.pointkey = b.pointkey and b.channel = 9 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_9, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_7_13
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 



------>>>>>> US STATIONS - CHANNEL 10

insert into ia.tvsoftware_pairwise_result_7_13

select y.orig_facilityid, ch_10 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_10, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_7_13 a, ia.tvsoftware_interference_7_13 b
        where a.orig_facilityid = b.facilityid and a.ch_10 = b.channel and a.pointkey = b.pointkey and b.channel = 10 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_10, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_7_13
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 11

insert into ia.tvsoftware_pairwise_result_7_13

select y.orig_facilityid, ch_11 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_11, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_7_13 a, ia.tvsoftware_interference_7_13 b
        where a.orig_facilityid = b.facilityid and a.ch_11 = b.channel and a.pointkey = b.pointkey and b.channel = 11 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_11, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_7_13
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  


------>>>>>> US STATIONS - CHANNEL 12

insert into ia.tvsoftware_pairwise_result_7_13

select y.orig_facilityid, ch_12 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_12, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_7_13 a, ia.tvsoftware_interference_7_13 b
        where a.orig_facilityid = b.facilityid and a.ch_12 = b.channel and a.pointkey = b.pointkey and b.channel = 12 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_12, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_7_13
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 



------>>>>>> US STATIONS - CHANNEL 13

insert into ia.tvsoftware_pairwise_result_7_13

select y.orig_facilityid, ch_13 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_13, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_7_13 a, ia.tvsoftware_interference_7_13 b
        where a.orig_facilityid = b.facilityid and a.ch_13 = b.channel and a.pointkey = b.pointkey and b.channel = 13 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_13, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_7_13
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> CANADIAN STATIONS - CHANNEL 7

insert into ia.tvsoftware_pairwise_result_7_13

select y.orig_facilityid, ch_7 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_7, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_7_13 a, ia.tvsoftware_interference_7_13 b
        where a.orig_facilityid = b.facilityid and a.ch_7 = b.channel and a.pointkey = b.pointkey and b.channel = 7 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_7, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_7_13
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;


------>>>>>> CANADIAN STATIONS - CHANNEL 8

insert into ia.tvsoftware_pairwise_result_7_13

select y.orig_facilityid, ch_8 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_8, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_7_13 a, ia.tvsoftware_interference_7_13 b
        where a.orig_facilityid = b.facilityid and a.ch_8 = b.channel and a.pointkey = b.pointkey and b.channel = 8 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_8, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_7_13
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> CANADIAN STATIONS - CHANNEL 9

insert into ia.tvsoftware_pairwise_result_7_13

select y.orig_facilityid, ch_9 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_9, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_7_13 a, ia.tvsoftware_interference_7_13 b
        where a.orig_facilityid = b.facilityid and a.ch_9 = b.channel and a.pointkey = b.pointkey and b.channel = 9 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_9, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_7_13
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> CANADIAN STATIONS - CHANNEL 10

insert into ia.tvsoftware_pairwise_result_7_13

select y.orig_facilityid, ch_10 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_10, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_7_13 a, ia.tvsoftware_interference_7_13 b
        where a.orig_facilityid = b.facilityid and a.ch_10 = b.channel and a.pointkey = b.pointkey and b.channel = 10 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_10, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_7_13
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  


------>>>>>> CANADIAN STATIONS - CHANNEL 11

insert into ia.tvsoftware_pairwise_result_7_13

select y.orig_facilityid, ch_11 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_11, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_7_13 a, ia.tvsoftware_interference_7_13 b
        where a.orig_facilityid = b.facilityid and a.ch_11 = b.channel and a.pointkey = b.pointkey and b.channel = 11 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_11, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_7_13
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  


------>>>>>> CANADIAN STATIONS - CHANNEL 12

insert into ia.tvsoftware_pairwise_result_7_13

select y.orig_facilityid, ch_12 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_12, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_7_13 a, ia.tvsoftware_interference_7_13 b
        where a.orig_facilityid = b.facilityid and a.ch_12 = b.channel and a.pointkey = b.pointkey and b.channel = 12 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_12, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_7_13
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> CANADIAN STATIONS - CHANNEL 13

insert into ia.tvsoftware_pairwise_result_7_13

select y.orig_facilityid, ch_13 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_13, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_7_13 a, ia.tvsoftware_interference_7_13 b
        where a.orig_facilityid = b.facilityid and a.ch_13 = b.channel and a.pointkey = b.pointkey and b.channel = 13 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_13, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_7_13
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 
