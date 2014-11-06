------------------------------------------------------------------------------------------------------------
----AUTHOR:  FEDERAL COMMUNICATIONS COMMISSION (FCC)                                                    ---- 
----LAST MODIFIED:  OCTOBER 31, 2014                                                                    ----
------------------------------------------------------------------------------------------------------------

---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
--------- ******* PROCESSING OF CHANNELS 5, 6, 33-36 ******* ---------
---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
---------------------------------------------------------------------------------------------------
-------STEP 1:  LOAD SERVICE AND INTERFERENCE POINT DATA FROM CHANNEL 5, 6, 33-36 DATA FILES-------
---------------------------------------------------------------------------------------------------

-----THIS CREATES TABLE INTO WHICH WE LOAD THE SERVICE.CSV FILE FROM THE CHANNEL 5, 6, 33-36 RUN-----
drop table if exists ia.tvsoftware_service_5_6_33_36; commit; 
create table ia.tvsoftware_service_5_6_33_36
(
pointkey integer,
facilityid integer,
channel integer,
serviceFlag integer
); commit;


-----THIS CREATES TABLE INTO WHICH WE LOAD THE FIRST INTERFERENCE.CSV FILE-----
drop table if exists ia.tvsoftware_interference_5_6_33_36; commit;  
create table ia.tvsoftware_interference_5_6_33_36
(
pointKey integer,
facilityID integer,
channel integer,
interferingFacilityID integer,
interferingChannel integer
); commit;

--->>>>> COPIES SERVICE.CSV INTO TABLE <<<<<---
\copy ia.tvsoftware_service_5_6_33_36 from 'tvstudyprocessing/ch05063336/service.csv' delimiter ',' csv; commit; 

--->>>>> COPIES INTERFERENCE.CSV INTO TABLE <<<<<---
\copy ia.tvsoftware_interference_5_6_33_36 from 'tvstudyprocessing/ch05063336/interference.csv' delimiter ',' csv; commit; 

--->>>>> CREATES INDEXES ON FREQUENTLY USED COLUMNS <<<<<---
create index tvsoftware_service_5_6_33_36_pointkey on ia.tvsoftware_service_5_6_33_36 (pointkey); commit; 
create index tvsoftware_service_5_6_33_36_channel on ia.tvsoftware_service_5_6_33_36 (channel); commit; 
create index tvsoftware_service_5_6_33_36_facilityid on ia.tvsoftware_service_5_6_33_36 (facilityid); commit; 
create index tvsoftware_interference_5_6_33_36_pointkey on ia.tvsoftware_interference_5_6_33_36 (pointkey); commit; 
create index tvsoftware_interference_5_6_33_36_channel on ia.tvsoftware_interference_5_6_33_36 (channel); commit; 
create index tvsoftware_interference_5_6_33_36_facilityid on ia.tvsoftware_interference_5_6_33_36 (facilityid); commit;


----------------------------------------------------------------------------------------------------------------------------------
--------STEP 2:  CREATE SINGLE TABLE THAT IDENTIFIES SERVICE POINTS OF ALL STATIONS REPLICATED ONTO CHANNELS 5, 6, 33-36 --------- 
----------------------------------------------------------------------------------------------------------------------------------

--->>>>> FOR US STATIONS, CREATES STAGING TABLE OF SERVICE POINTS BASED ON BASELINE POINTS <<<<<---
drop table if exists ia.tvsoftware_analysis_orig_rep_5_6_33_36; commit; 
create table ia.tvsoftware_analysis_orig_rep_5_6_33_36
(
orig_facilityid integer,
pointkey integer,
population numeric, 
orig_channel integer,
orig_serviceflag integer,
interference_free_flag integer,
ch_5 integer,
ch_5_serviceflag integer,
ch_6 integer,
ch_6_serviceflag integer,
ch_33 integer,
ch_33_serviceflag integer,
ch_34 integer,
ch_34_serviceflag integer,
ch_35 integer,
ch_35_serviceflag integer,
ch_36 integer,
ch_36_serviceflag integer
); commit;


insert into ia.tvsoftware_analysis_orig_rep_5_6_33_36

select a.orig_facilityid, a.pointkey, population, orig_channel, orig_serviceflag, interference_free_flag,
b.channel as ch_5, b.serviceflag as ch_5_serviceflag,
c.channel as ch_6, c.serviceflag as ch_6_serviceflag, 
d.channel as ch_33, d.serviceflag as ch_33_serviceflag,
e.channel as ch_34, e.serviceflag as ch_34_serviceflag,
f.channel as ch_35, f.serviceflag as ch_35_serviceflag,
g.channel as ch_36, g.serviceflag as ch_36_serviceflag
from
(       
	select orig_facilityid, pointkey, population, orig_channel, orig_serviceflag, interference_free_flag
	from ia.tvsoftware_rep_station_points
) a
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_5_6_33_36
	where channel = 5 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) b
on a.orig_facilityid = b.facilityid and a.pointkey = b.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_5_6_33_36
	where channel = 6 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) c
on a.orig_facilityid = c.facilityid and a.pointkey = c.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_5_6_33_36
	where channel = 33 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) d
on a.orig_facilityid = d.facilityid and a.pointkey = d.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_5_6_33_36
	where channel = 34 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) e
on a.orig_facilityid = e.facilityid and a.pointkey = e.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_5_6_33_36
	where channel = 35 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) f
on a.orig_facilityid = f.facilityid and a.pointkey = f.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_5_6_33_36
	where channel = 36 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) g
on a.orig_facilityid = g.facilityid and a.pointkey = g.pointkey; commit; 


create index tvsoftware_analysis_orig_rep_5_6_33_36_orig_facilityid on ia.tvsoftware_analysis_orig_rep_5_6_33_36 (orig_facilityid); commit; 
create index tvsoftware_analysis_orig_rep_5_6_33_36_pointkey on ia.tvsoftware_analysis_orig_rep_5_6_33_36 (pointkey); commit; 
create index tvsoftware_analysis_orig_rep_5_6_33_36_orig_channel on ia.tvsoftware_analysis_orig_rep_5_6_33_36 (orig_channel); commit; 
create index tvsoftware_analysis_orig_rep_5_6_33_36_ch_5 on ia.tvsoftware_analysis_orig_rep_5_6_33_36 (ch_5); commit; 
create index tvsoftware_analysis_orig_rep_5_6_33_36_ch_6 on ia.tvsoftware_analysis_orig_rep_5_6_33_36 (ch_6); commit; 
create index tvsoftware_analysis_orig_rep_5_6_33_36_ch_33 on ia.tvsoftware_analysis_orig_rep_5_6_33_36 (ch_33); commit; 
create index tvsoftware_analysis_orig_rep_5_6_33_36_ch_34 on ia.tvsoftware_analysis_orig_rep_5_6_33_36 (ch_34); commit; 
create index tvsoftware_analysis_orig_rep_5_6_33_36_ch_35 on ia.tvsoftware_analysis_orig_rep_5_6_33_36 (ch_35); commit; 
create index tvsoftware_analysis_orig_rep_5_6_33_36_ch_36 on ia.tvsoftware_analysis_orig_rep_5_6_33_36 (ch_36); commit; 
create index tvsoftware_analysis_orig_rep_5_6_33_36_interference_free_flag on ia.tvsoftware_analysis_orig_rep_5_6_33_36 (interference_free_flag); commit;



--->>>>> FOR CANADIAN STATIONS, CREATES STAGING TABLE OF SERVICE POINTS BASED ON BASELINE POINTS <<<<<---
drop table if exists ia.tvsoftware_analysis_canada_rep_5_6_33_36; commit; 
create table ia.tvsoftware_analysis_canada_rep_5_6_33_36
(
orig_facilityid integer,
pointkey integer,
population numeric, 
orig_channel integer,
orig_serviceflag integer,
interference_free_flag integer,
ch_5 integer,
ch_5_serviceflag integer,
ch_6 integer,
ch_6_serviceflag integer,
ch_33 integer,
ch_33_serviceflag integer,
ch_34 integer,
ch_34_serviceflag integer,
ch_35 integer,
ch_35_serviceflag integer,
ch_36 integer,
ch_36_serviceflag integer
); commit;


insert into ia.tvsoftware_analysis_canada_rep_5_6_33_36

select a.orig_facilityid, a.pointkey, population, orig_channel, orig_serviceflag, interference_free_flag,
b.channel as ch_5, b.serviceflag as ch_5_serviceflag,
c.channel as ch_6, c.serviceflag as ch_6_serviceflag, 
d.channel as ch_33, d.serviceflag as ch_33_serviceflag,
e.channel as ch_34, e.serviceflag as ch_34_serviceflag,
f.channel as ch_35, f.serviceflag as ch_35_serviceflag,
g.channel as ch_36, g.serviceflag as ch_36_serviceflag
from
(       
	select orig_facilityid, pointkey, population, orig_channel, orig_serviceflag, interference_free_flag
	from ia.tvsoftware_rep_station_points_canada
) a
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_5_6_33_36
	where channel = 5 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) b
on a.orig_facilityid = b.facilityid and a.pointkey = b.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_5_6_33_36
	where channel = 6 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA')
) c
on a.orig_facilityid = c.facilityid and a.pointkey = c.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_5_6_33_36
	where channel = 33 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) d
on a.orig_facilityid = d.facilityid and a.pointkey = d.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_5_6_33_36
	where channel = 34 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) e
on a.orig_facilityid = e.facilityid and a.pointkey = e.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_5_6_33_36
	where channel = 35 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) f
on a.orig_facilityid = f.facilityid and a.pointkey = f.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_5_6_33_36
	where channel = 36 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) g
on a.orig_facilityid = g.facilityid and a.pointkey = g.pointkey; commit; 


create index tvsoftware_analysis_canada_rep_5_6_33_36_orig_facilityid on ia.tvsoftware_analysis_canada_rep_5_6_33_36 (orig_facilityid); commit; 
create index tvsoftware_analysis_canada_rep_5_6_33_36_pointkey on ia.tvsoftware_analysis_canada_rep_5_6_33_36 (pointkey); commit; 
create index tvsoftware_analysis_canada_rep_5_6_33_36_orig_channel on ia.tvsoftware_analysis_canada_rep_5_6_33_36 (orig_channel); commit; 
create index tvsoftware_analysis_canada_rep_5_6_33_36_ch_5 on ia.tvsoftware_analysis_canada_rep_5_6_33_36 (ch_5); commit; 
create index tvsoftware_analysis_canada_rep_5_6_33_36_ch_6 on ia.tvsoftware_analysis_canada_rep_5_6_33_36 (ch_6); commit; 
create index tvsoftware_analysis_canada_rep_5_6_33_36_ch_33 on ia.tvsoftware_analysis_canada_rep_5_6_33_36 (ch_33); commit; 
create index tvsoftware_analysis_canada_rep_5_6_33_36_ch_34 on ia.tvsoftware_analysis_canada_rep_5_6_33_36 (ch_34); commit; 
create index tvsoftware_analysis_canada_rep_5_6_33_36_ch_35 on ia.tvsoftware_analysis_canada_rep_5_6_33_36 (ch_35); commit; 
create index tvsoftware_analysis_canada_rep_5_6_33_36_ch_36 on ia.tvsoftware_analysis_canada_rep_5_6_33_36 (ch_36); commit; 
create index tvsoftware_analysis_canada_rep_5_6_33_36_interference_free_flag on ia.tvsoftware_analysis_canada_rep_5_6_33_36 (interference_free_flag); commit;


--------------------------------------------------------------------------------------------------------------
--------STEP 3:  CALCULATE PAIRWISE INTERFERENCE PERCENTAGES FOR ALL STATIONS ON CHANNELS 5, 6, 33-36--------- 
--------------------------------------------------------------------------------------------------------------
drop table if exists ia.tvsoftware_pairwise_result_5_6_33_36; commit;
create table ia.tvsoftware_pairwise_result_5_6_33_36 
(
orig_facilityid integer,
orig_channel integer,
interferingfacilityid integer,
interferingchannel integer,
interference_population numeric,
interference_free_pop numeric,
interference_pct numeric
); commit;

------>>>>>> US STATIONS - CHANNEL 5

insert into ia.tvsoftware_pairwise_result_5_6_33_36

select y.orig_facilityid, ch_5 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop,
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_5, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_5_6_33_36 a, ia.tvsoftware_interference_5_6_33_36 b
        where a.orig_facilityid = b.facilityid and a.ch_5 = b.channel and a.pointkey = b.pointkey and b.channel = 5 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_5, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_5_6_33_36
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> US STATIONS - CHANNEL 6

insert into ia.tvsoftware_pairwise_result_5_6_33_36

select y.orig_facilityid, ch_6 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_6, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_5_6_33_36 a, ia.tvsoftware_interference_5_6_33_36 b
        where a.orig_facilityid = b.facilityid and a.ch_6 = b.channel and a.pointkey = b.pointkey and b.channel = 6 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_6, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_5_6_33_36
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> US STATIONS - CHANNEL 33

insert into ia.tvsoftware_pairwise_result_5_6_33_36

select y.orig_facilityid, ch_33 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_33, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_5_6_33_36 a, ia.tvsoftware_interference_5_6_33_36 b
        where a.orig_facilityid = b.facilityid and a.ch_33 = b.channel and a.pointkey = b.pointkey and b.channel = 33 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_33, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_5_6_33_36
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> US STATIONS - CHANNEL 34

insert into ia.tvsoftware_pairwise_result_5_6_33_36

select y.orig_facilityid, ch_34 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_34, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_5_6_33_36 a, ia.tvsoftware_interference_5_6_33_36 b
        where a.orig_facilityid = b.facilityid and a.ch_34 = b.channel and a.pointkey = b.pointkey and b.channel = 34 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_34, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_5_6_33_36
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> US STATIONS - CHANNEL 35

insert into ia.tvsoftware_pairwise_result_5_6_33_36

select y.orig_facilityid, ch_35 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_35, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_5_6_33_36 a, ia.tvsoftware_interference_5_6_33_36 b
        where a.orig_facilityid = b.facilityid and a.ch_35 = b.channel and a.pointkey = b.pointkey and b.channel = 35 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_35, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_5_6_33_36
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> US STATIONS - CHANNEL 36

insert into ia.tvsoftware_pairwise_result_5_6_33_36

select y.orig_facilityid, ch_36 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_36, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_5_6_33_36 a, ia.tvsoftware_interference_5_6_33_36 b
        where a.orig_facilityid = b.facilityid and a.ch_36 = b.channel and a.pointkey = b.pointkey and b.channel = 36 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_36, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_5_6_33_36
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> CANADIAN STATIONS - CHANNEL 5

insert into ia.tvsoftware_pairwise_result_5_6_33_36

select y.orig_facilityid, ch_5 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_5, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_5_6_33_36 a, ia.tvsoftware_interference_5_6_33_36 b
        where a.orig_facilityid = b.facilityid and a.ch_5 = b.channel and a.pointkey = b.pointkey and b.channel = 5 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_5, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_5_6_33_36
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> CANADIAN STATIONS - CHANNEL 6

insert into ia.tvsoftware_pairwise_result_5_6_33_36

select y.orig_facilityid, ch_6 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_6, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_5_6_33_36 a, ia.tvsoftware_interference_5_6_33_36 b
        where a.orig_facilityid = b.facilityid and a.ch_6 = b.channel and a.pointkey = b.pointkey and b.channel = 6 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_6, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_5_6_33_36
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> CANADIAN STATIONS - CHANNEL 33

insert into ia.tvsoftware_pairwise_result_5_6_33_36

select y.orig_facilityid, ch_33 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_33, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_5_6_33_36 a, ia.tvsoftware_interference_5_6_33_36 b
        where a.orig_facilityid = b.facilityid and a.ch_33 = b.channel and a.pointkey = b.pointkey and b.channel = 33 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_33, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_5_6_33_36
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> CANADIAN STATIONS - CHANNEL 34

insert into ia.tvsoftware_pairwise_result_5_6_33_36

select y.orig_facilityid, ch_34 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_34, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_5_6_33_36 a, ia.tvsoftware_interference_5_6_33_36 b
        where a.orig_facilityid = b.facilityid and a.ch_34 = b.channel and a.pointkey = b.pointkey and b.channel = 34 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_34, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_5_6_33_36
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> CANADIAN STATIONS - CHANNEL 35

insert into ia.tvsoftware_pairwise_result_5_6_33_36

select y.orig_facilityid, ch_35 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_35, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_5_6_33_36 a, ia.tvsoftware_interference_5_6_33_36 b
        where a.orig_facilityid = b.facilityid and a.ch_35 = b.channel and a.pointkey = b.pointkey and b.channel = 35 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_35, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_5_6_33_36
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> CANADIAN STATIONS - CHANNEL 36

insert into ia.tvsoftware_pairwise_result_5_6_33_36

select y.orig_facilityid, ch_36 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_36, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_5_6_33_36 a, ia.tvsoftware_interference_5_6_33_36 b
        where a.orig_facilityid = b.facilityid and a.ch_36 = b.channel and a.pointkey = b.pointkey and b.channel = 36 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_36, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_5_6_33_36
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 
