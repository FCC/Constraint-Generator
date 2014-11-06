------------------------------------------------------------------------------------------------------------
----AUTHOR:  FEDERAL COMMUNICATIONS COMMISSION (FCC)                                                    ---- 
----LAST MODIFIED:  OCTOBER 31, 2014                                                                    ----
------------------------------------------------------------------------------------------------------------

---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
--------- ******* PROCESSING OF CHANNELS 27-32 ******* ---------
---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
---------------------------------------------------------------------------------------------
-------STEP 1:  LOAD SERVICE AND INTERFERENCE POINT DATA FROM CHANNEL 27-32 DATA FILES-------
---------------------------------------------------------------------------------------------

-----THIS CREATES TABLE INTO WHICH WE LOAD THE SERVICE.CSV FILE FROM THE CHANNEL 27-32 RUN-----
drop table if exists ia.tvsoftware_service_27_32; commit;  
create table ia.tvsoftware_service_27_32
(
pointkey integer,
facilityid integer,
channel integer,
serviceFlag integer
); commit;


-----THIS CREATES TABLE INTO WHICH WE LOAD THE FIRST INTERFERENCE.CSV FILE-----
drop table if exists ia.tvsoftware_interference_27_32; commit;  
create table ia.tvsoftware_interference_27_32
(
pointKey integer,
facilityID integer,
channel integer,
interferingFacilityID integer,
interferingChannel integer
); commit;

--->>>>> COPIES SERVICE.CSV INTO TABLE <<<<<---
\copy ia.tvsoftware_service_27_32 from 'tvstudyprocessing/ch2732/service.csv' delimiter ',' csv; commit; 

--->>>>> COPIES INTERFERENCE.CSV INTO TABLE <<<<<---
\copy ia.tvsoftware_interference_27_32 from 'tvstudyprocessing/ch2732/interference.csv' delimiter ',' csv; commit; 

--->>>>> CREATES INDEXES ON FREQUENTLY USED COLUMNS <<<<<---
create index tvsoftware_service_27_32_pointkey on ia.tvsoftware_service_27_32 (pointkey); commit; 
create index tvsoftware_service_27_32_channel on ia.tvsoftware_service_27_32 (channel); commit; 
create index tvsoftware_service_27_32_facilityid on ia.tvsoftware_service_27_32 (facilityid); commit; 
create index tvsoftware_interference_27_32_pointkey on ia.tvsoftware_interference_27_32 (pointkey); commit; 
create index tvsoftware_interference_27_32_channel on ia.tvsoftware_interference_27_32 (channel); commit; 
create index tvsoftware_interference_27_32_facilityid on ia.tvsoftware_interference_27_32 (facilityid); commit;


------------------------------------------------------------------------------------------------------------------------------
--------STEP 2:  CREATE SINGLE TABLE THAT IDENTIFIES SERVICE POINTS OF ALL STATIONS REPLICATED ONTO CHANNELS 27 - 32 --------- 
------------------------------------------------------------------------------------------------------------------------------

--->>>>> FOR US STATIONS, CREATES STAGING TABLE OF SERVICE POINTS BASED ON BASELINE POINTS <<<<<---
drop table if exists ia.tvsoftware_analysis_orig_rep_27_32; commit; 
create table ia.tvsoftware_analysis_orig_rep_27_32
(
orig_facilityid integer,
pointkey integer,
population numeric, 
orig_channel integer,
orig_serviceflag integer,
interference_free_flag integer,
ch_27 integer,
ch_27_serviceflag integer,
ch_28 integer,
ch_28_serviceflag integer,
ch_29 integer,
ch_29_serviceflag integer,
ch_30 integer,
ch_30_serviceflag integer,
ch_31 integer,
ch_31_serviceflag integer,
ch_32 integer,
ch_32_serviceflag integer
); commit;


insert into ia.tvsoftware_analysis_orig_rep_27_32

select a.orig_facilityid, a.pointkey, population, orig_channel, orig_serviceflag, interference_free_flag,
b.channel as ch_27, b.serviceflag as ch_27_serviceflag,
c.channel as ch_28, c.serviceflag as ch_28_serviceflag, 
d.channel as ch_29, d.serviceflag as ch_29_serviceflag,
e.channel as ch_30, e.serviceflag as ch_30_serviceflag,
f.channel as ch_31, f.serviceflag as ch_31_serviceflag,
g.channel as ch_32, g.serviceflag as ch_32_serviceflag
from
(       
	select orig_facilityid, pointkey, population, orig_channel, orig_serviceflag, interference_free_flag
	from ia.tvsoftware_rep_station_points
) a
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_27_32
	where channel = 27 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) b
on a.orig_facilityid = b.facilityid and a.pointkey = b.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_27_32
	where channel = 28 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) c
on a.orig_facilityid = c.facilityid and a.pointkey = c.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_27_32
	where channel = 29 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) d
on a.orig_facilityid = d.facilityid and a.pointkey = d.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_27_32
	where channel = 30 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) e
on a.orig_facilityid = e.facilityid and a.pointkey = e.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_27_32
	where channel = 31 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) f
on a.orig_facilityid = f.facilityid and a.pointkey = f.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_27_32
	where channel = 32 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) g
on a.orig_facilityid = g.facilityid and a.pointkey = g.pointkey; commit; 


create index tvsoftware_analysis_orig_rep_27_32_orig_facilityid on ia.tvsoftware_analysis_orig_rep_27_32 (orig_facilityid); commit; 
create index tvsoftware_analysis_orig_rep_27_32_pointkey on ia.tvsoftware_analysis_orig_rep_27_32 (pointkey); commit; 
create index tvsoftware_analysis_orig_rep_27_32_orig_channel on ia.tvsoftware_analysis_orig_rep_27_32 (orig_channel); commit; 
create index tvsoftware_analysis_orig_rep_27_32_ch_27 on ia.tvsoftware_analysis_orig_rep_27_32 (ch_27); commit; 
create index tvsoftware_analysis_orig_rep_27_32_ch_28 on ia.tvsoftware_analysis_orig_rep_27_32 (ch_28); commit; 
create index tvsoftware_analysis_orig_rep_27_32_ch_29 on ia.tvsoftware_analysis_orig_rep_27_32 (ch_29); commit; 
create index tvsoftware_analysis_orig_rep_27_32_ch_30 on ia.tvsoftware_analysis_orig_rep_27_32 (ch_30); commit; 
create index tvsoftware_analysis_orig_rep_27_32_ch_31 on ia.tvsoftware_analysis_orig_rep_27_32 (ch_31); commit; 
create index tvsoftware_analysis_orig_rep_27_32_ch_32 on ia.tvsoftware_analysis_orig_rep_27_32 (ch_32); commit; 
create index tvsoftware_analysis_orig_rep_27_32_interference_free_flag on ia.tvsoftware_analysis_orig_rep_27_32 (interference_free_flag); commit;


--->>>>> FOR CANADIAN STATIONS, CREATES STAGING TABLE OF SERVICE POINTS BASED ON BASELINE POINTS <<<<<---
drop table if exists ia.tvsoftware_analysis_canada_rep_27_32; commit; 
create table ia.tvsoftware_analysis_canada_rep_27_32
(
orig_facilityid integer,
pointkey integer,
population numeric, 
orig_channel integer,
orig_serviceflag integer,
interference_free_flag integer,
ch_27 integer,
ch_27_serviceflag integer,
ch_28 integer,
ch_28_serviceflag integer,
ch_29 integer,
ch_29_serviceflag integer,
ch_30 integer,
ch_30_serviceflag integer,
ch_31 integer,
ch_31_serviceflag integer,
ch_32 integer,
ch_32_serviceflag integer
); commit;


insert into ia.tvsoftware_analysis_canada_rep_27_32

select a.orig_facilityid, a.pointkey, population, orig_channel, orig_serviceflag, interference_free_flag,
b.channel as ch_27, b.serviceflag as ch_27_serviceflag,
c.channel as ch_28, c.serviceflag as ch_28_serviceflag, 
d.channel as ch_29, d.serviceflag as ch_29_serviceflag,
e.channel as ch_30, e.serviceflag as ch_30_serviceflag,
f.channel as ch_31, f.serviceflag as ch_31_serviceflag,
g.channel as ch_32, g.serviceflag as ch_32_serviceflag
from
(       
	select orig_facilityid, pointkey, population, orig_channel, orig_serviceflag, interference_free_flag
	from ia.tvsoftware_rep_station_points_canada
) a
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_27_32
	where channel = 27 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) b
on a.orig_facilityid = b.facilityid and a.pointkey = b.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_27_32
	where channel = 28 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) c
on a.orig_facilityid = c.facilityid and a.pointkey = c.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_27_32
	where channel = 29 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) d
on a.orig_facilityid = d.facilityid and a.pointkey = d.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_27_32
	where channel = 30 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) e
on a.orig_facilityid = e.facilityid and a.pointkey = e.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_27_32
	where channel = 31 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) f
on a.orig_facilityid = f.facilityid and a.pointkey = f.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_27_32
	where channel = 32 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) g
on a.orig_facilityid = g.facilityid and a.pointkey = g.pointkey; commit; 


create index tvsoftware_analysis_canada_rep_27_32_orig_facilityid on ia.tvsoftware_analysis_canada_rep_27_32 (orig_facilityid); commit; 
create index tvsoftware_analysis_canada_rep_27_32_pointkey on ia.tvsoftware_analysis_canada_rep_27_32 (pointkey); commit; 
create index tvsoftware_analysis_canada_rep_27_32_orig_channel on ia.tvsoftware_analysis_canada_rep_27_32 (orig_channel); commit; 
create index tvsoftware_analysis_canada_rep_27_32_ch_27 on ia.tvsoftware_analysis_canada_rep_27_32 (ch_27); commit; 
create index tvsoftware_analysis_canada_rep_27_32_ch_28 on ia.tvsoftware_analysis_canada_rep_27_32 (ch_28); commit; 
create index tvsoftware_analysis_canada_rep_27_32_ch_29 on ia.tvsoftware_analysis_canada_rep_27_32 (ch_29); commit; 
create index tvsoftware_analysis_canada_rep_27_32_ch_30 on ia.tvsoftware_analysis_canada_rep_27_32 (ch_30); commit; 
create index tvsoftware_analysis_canada_rep_27_32_ch_31 on ia.tvsoftware_analysis_canada_rep_27_32 (ch_31); commit; 
create index tvsoftware_analysis_canada_rep_27_32_ch_32 on ia.tvsoftware_analysis_canada_rep_27_32 (ch_32); commit; 
create index tvsoftware_analysis_canada_rep_27_32_interference_free_flag on ia.tvsoftware_analysis_canada_rep_27_32 (interference_free_flag); commit;

--------------------------------------------------------------------------------------------------------
--------STEP 3:  CALCULATE PAIRWISE INTERFERENCE PERCENTAGES FOR ALL STATIONS ON CHANNELS 27-32--------- 
--------------------------------------------------------------------------------------------------------
drop table if exists ia.tvsoftware_pairwise_result_27_32; commit; 
create table ia.tvsoftware_pairwise_result_27_32 
(
orig_facilityid integer,
orig_channel integer,
interferingfacilityid integer,
interferingchannel integer,
interference_population numeric,
interference_free_pop numeric,
interference_pct numeric
); commit;


------>>>>>> US STATIONS - CHANNEL 27

insert into ia.tvsoftware_pairwise_result_27_32

select y.orig_facilityid, ch_27 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_27, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_27_32 a, ia.tvsoftware_interference_27_32 b
        where a.orig_facilityid = b.facilityid and a.ch_27 = b.channel and a.pointkey = b.pointkey and b.channel = 27 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_27, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_27_32
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 28

insert into ia.tvsoftware_pairwise_result_27_32

select y.orig_facilityid, ch_28 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_28, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_27_32 a, ia.tvsoftware_interference_27_32 b
        where a.orig_facilityid = b.facilityid and a.ch_28 = b.channel and a.pointkey = b.pointkey and b.channel = 28 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_28, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_27_32
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 29

insert into ia.tvsoftware_pairwise_result_27_32

select y.orig_facilityid, ch_29 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_29, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_27_32 a, ia.tvsoftware_interference_27_32 b
        where a.orig_facilityid = b.facilityid and a.ch_29 = b.channel and a.pointkey = b.pointkey and b.channel = 29 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_29, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_27_32
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 30

insert into ia.tvsoftware_pairwise_result_27_32

select y.orig_facilityid, ch_30 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_30, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_27_32 a, ia.tvsoftware_interference_27_32 b
        where a.orig_facilityid = b.facilityid and a.ch_30 = b.channel and a.pointkey = b.pointkey and b.channel = 30 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_30, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_27_32
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 31

insert into ia.tvsoftware_pairwise_result_27_32

select y.orig_facilityid, ch_31 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_31, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_27_32 a, ia.tvsoftware_interference_27_32 b
        where a.orig_facilityid = b.facilityid and a.ch_31 = b.channel and a.pointkey = b.pointkey and b.channel = 31 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_31, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_27_32
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 32

insert into ia.tvsoftware_pairwise_result_27_32

select y.orig_facilityid, ch_32 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_32, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_27_32 a, ia.tvsoftware_interference_27_32 b
        where a.orig_facilityid = b.facilityid and a.ch_32 = b.channel and a.pointkey = b.pointkey and b.channel = 32 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_32, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_27_32
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  


------>>>>>> CANADIAN STATIONS - CHANNEL 27

insert into ia.tvsoftware_pairwise_result_27_32

select y.orig_facilityid, ch_27 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_27, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_27_32 a, ia.tvsoftware_interference_27_32 b
        where a.orig_facilityid = b.facilityid and a.ch_27 = b.channel and a.pointkey = b.pointkey and b.channel = 27 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_27, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_27_32
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> CANADIAN STATIONS - CHANNEL 28

insert into ia.tvsoftware_pairwise_result_27_32

select y.orig_facilityid, ch_28 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_28, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_27_32 a, ia.tvsoftware_interference_27_32 b
        where a.orig_facilityid = b.facilityid and a.ch_28 = b.channel and a.pointkey = b.pointkey and b.channel = 28 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_28, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_27_32
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> CANADIAN STATIONS - CHANNEL 29

insert into ia.tvsoftware_pairwise_result_27_32

select y.orig_facilityid, ch_29 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_29, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_27_32 a, ia.tvsoftware_interference_27_32 b
        where a.orig_facilityid = b.facilityid and a.ch_29 = b.channel and a.pointkey = b.pointkey and b.channel = 29 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_29, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_27_32
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> CANADIAN STATIONS - CHANNEL 30

insert into ia.tvsoftware_pairwise_result_27_32

select y.orig_facilityid, ch_30 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_30, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_27_32 a, ia.tvsoftware_interference_27_32 b
        where a.orig_facilityid = b.facilityid and a.ch_30 = b.channel and a.pointkey = b.pointkey and b.channel = 30 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_30, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_27_32
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  


------>>>>>> CANADIAN STATIONS - CHANNEL 31

insert into ia.tvsoftware_pairwise_result_27_32

select y.orig_facilityid, ch_31 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_31, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_27_32 a, ia.tvsoftware_interference_27_32 b
        where a.orig_facilityid = b.facilityid and a.ch_31 = b.channel and a.pointkey = b.pointkey and b.channel = 31 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_31, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_27_32
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> CANADIAN STATIONS - CHANNEL 32

insert into ia.tvsoftware_pairwise_result_27_32

select y.orig_facilityid, ch_32 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_32, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_27_32 a, ia.tvsoftware_interference_27_32 b
        where a.orig_facilityid = b.facilityid and a.ch_32 = b.channel and a.pointkey = b.pointkey and b.channel = 32 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_32, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_27_32
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  
