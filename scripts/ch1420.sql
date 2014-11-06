------------------------------------------------------------------------------------------------------------
----AUTHOR:  FEDERAL COMMUNICATIONS COMMISSION (FCC)                                                    ---- 
----LAST MODIFIED:  OCTOBER 31, 2014                                                                    ----
------------------------------------------------------------------------------------------------------------

---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
--------- ******* PROCESSING OF CHANNELS 14-20 ******* ---------
---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
---------------------------------------------------------------------------------------------
-------STEP 1:  LOAD SERVICE AND INTERFERENCE POINT DATA FROM CHANNEL 14-20 DATA FILES-------
---------------------------------------------------------------------------------------------

-----THIS CREATES TABLE INTO WHICH WE LOAD THE SERVICE.CSV FILE FROM THE CHANNEL 14-20 RUN-----
drop table if exists ia.tvsoftware_service_14_20; commit; 
create table ia.tvsoftware_service_14_20
(
pointkey integer,
facilityid integer,
channel integer,
serviceFlag integer
); commit;


-----THIS CREATES TABLE INTO WHICH WE LOAD THE FIRST INTERFERENCE.CSV FILE-----
drop table if exists ia.tvsoftware_interference_14_20; commit; 
create table ia.tvsoftware_interference_14_20
(
pointKey integer,
facilityID integer,
channel integer,
interferingFacilityID integer,
interferingChannel integer
); commit;

--->>>>> COPIES SERVICE.CSV INTO TABLE <<<<<---
\copy ia.tvsoftware_service_14_20 from 'tvstudyprocessing/ch1420/service.csv' delimiter ',' csv; commit; 

--->>>>> COPIES INTERFERENCE.CSV INTO TABLE <<<<<---
\copy ia.tvsoftware_interference_14_20 from 'tvstudyprocessing/ch1420/interference.csv' delimiter ',' csv; commit; 

--->>>>> CREATES INDEXES ON FREQUENTLY USED COLUMNS <<<<<---
create index tvsoftware_service_14_20_pointkey on ia.tvsoftware_service_14_20 (pointkey); commit; 
create index tvsoftware_service_14_20_channel on ia.tvsoftware_service_14_20 (channel); commit; 
create index tvsoftware_service_14_20_facilityid on ia.tvsoftware_service_14_20 (facilityid); commit; 
create index tvsoftware_interference_14_20_pointkey on ia.tvsoftware_interference_14_20 (pointkey); commit; 
create index tvsoftware_interference_14_20_channel on ia.tvsoftware_interference_14_20 (channel); commit; 
create index tvsoftware_interference_14_20_facilityid on ia.tvsoftware_interference_14_20 (facilityid); commit;


------------------------------------------------------------------------------------------------------------------------------
--------STEP 2:  CREATE SINGLE TABLE THAT IDENTIFIES SERVICE POINTS OF ALL STATIONS REPLICATED ONTO CHANNELS 14 - 20 --------- 
------------------------------------------------------------------------------------------------------------------------------

--->>>>> FOR US STATIONS, CREATES STAGING TABLE OF SERVICE POINTS BASED ON BASELINE POINTS <<<<<---
drop table if exists ia.tvsoftware_analysis_orig_rep_14_20; commit; 
create table ia.tvsoftware_analysis_orig_rep_14_20
(
orig_facilityid integer,
pointkey integer,
population numeric, 
orig_channel integer,
orig_serviceflag integer,
interference_free_flag integer,
ch_14 integer,
ch_14_serviceflag integer,
ch_15 integer,
ch_15_serviceflag integer,
ch_16 integer,
ch_16_serviceflag integer,
ch_17 integer,
ch_17_serviceflag integer,
ch_18 integer,
ch_18_serviceflag integer,
ch_19 integer,
ch_19_serviceflag integer,
ch_20 integer,
ch_20_serviceflag integer
); commit;


insert into ia.tvsoftware_analysis_orig_rep_14_20

select a.orig_facilityid, a.pointkey, population, orig_channel, orig_serviceflag, interference_free_flag,
b.channel as ch_14, b.serviceflag as ch_14_serviceflag,
c.channel as ch_15, c.serviceflag as ch_15_serviceflag, 
d.channel as ch_16, d.serviceflag as ch_16_serviceflag,
e.channel as ch_17, e.serviceflag as ch_17_serviceflag,
f.channel as ch_18, f.serviceflag as ch_18_serviceflag,
g.channel as ch_19, g.serviceflag as ch_19_serviceflag,
h.channel as ch_20, h.serviceflag as ch_20_serviceflag
from
(       
	select orig_facilityid, pointkey, population, orig_channel, orig_serviceflag, interference_free_flag
	from ia.tvsoftware_rep_station_points
) a
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_14_20
	where channel = 14 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) b
on a.orig_facilityid = b.facilityid and a.pointkey = b.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_14_20
	where channel = 15 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) c
on a.orig_facilityid = c.facilityid and a.pointkey = c.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_14_20
	where channel = 16 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) d
on a.orig_facilityid = d.facilityid and a.pointkey = d.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_14_20
	where channel = 17 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) e
on a.orig_facilityid = e.facilityid and a.pointkey = e.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_14_20
	where channel = 18 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) f
on a.orig_facilityid = f.facilityid and a.pointkey = f.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_14_20
	where channel = 19 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) g
on a.orig_facilityid = g.facilityid and a.pointkey = g.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_14_20
	where channel = 20 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) h
on a.orig_facilityid = h.facilityid and a.pointkey = h.pointkey; commit; 


create index tvsoftware_analysis_orig_rep_14_20_orig_facilityid on ia.tvsoftware_analysis_orig_rep_14_20 (orig_facilityid); commit; 
create index tvsoftware_analysis_orig_rep_14_20_pointkey on ia.tvsoftware_analysis_orig_rep_14_20 (pointkey); commit; 
create index tvsoftware_analysis_orig_rep_14_20_orig_channel on ia.tvsoftware_analysis_orig_rep_14_20 (orig_channel); commit; 
create index tvsoftware_analysis_orig_rep_14_20_ch_14 on ia.tvsoftware_analysis_orig_rep_14_20 (ch_14); commit; 
create index tvsoftware_analysis_orig_rep_14_20_ch_15 on ia.tvsoftware_analysis_orig_rep_14_20 (ch_15); commit; 
create index tvsoftware_analysis_orig_rep_14_20_ch_16 on ia.tvsoftware_analysis_orig_rep_14_20 (ch_16); commit; 
create index tvsoftware_analysis_orig_rep_14_20_ch_17 on ia.tvsoftware_analysis_orig_rep_14_20 (ch_17); commit; 
create index tvsoftware_analysis_orig_rep_14_20_ch_18 on ia.tvsoftware_analysis_orig_rep_14_20 (ch_18); commit; 
create index tvsoftware_analysis_orig_rep_14_20_ch_19 on ia.tvsoftware_analysis_orig_rep_14_20 (ch_19); commit; 
create index tvsoftware_analysis_orig_rep_14_20_ch_20 on ia.tvsoftware_analysis_orig_rep_14_20 (ch_20); commit; 
create index tvsoftware_analysis_orig_rep_14_20_interference_free_flag on ia.tvsoftware_analysis_orig_rep_14_20 (interference_free_flag); commit;


--->>>>> FOR CANADIAN STATIONS, CREATES STAGING TABLE OF SERVICE POINTS BASED ON BASELINE POINTS <<<<<---
drop table if exists ia.tvsoftware_analysis_canada_rep_14_20; commit; 
create table ia.tvsoftware_analysis_canada_rep_14_20
(
orig_facilityid integer,
pointkey integer,
population numeric, 
orig_channel integer,
orig_serviceflag integer,
interference_free_flag integer,
ch_14 integer,
ch_14_serviceflag integer,
ch_15 integer,
ch_15_serviceflag integer,
ch_16 integer,
ch_16_serviceflag integer,
ch_17 integer,
ch_17_serviceflag integer,
ch_18 integer,
ch_18_serviceflag integer,
ch_19 integer,
ch_19_serviceflag integer,
ch_20 integer,
ch_20_serviceflag integer
); commit;


insert into ia.tvsoftware_analysis_canada_rep_14_20

select a.orig_facilityid, a.pointkey, population, orig_channel, orig_serviceflag, interference_free_flag,
b.channel as ch_14, b.serviceflag as ch_14_serviceflag,
c.channel as ch_15, c.serviceflag as ch_15_serviceflag, 
d.channel as ch_16, d.serviceflag as ch_16_serviceflag,
e.channel as ch_17, e.serviceflag as ch_17_serviceflag,
f.channel as ch_18, f.serviceflag as ch_18_serviceflag,
g.channel as ch_19, g.serviceflag as ch_19_serviceflag,
h.channel as ch_20, h.serviceflag as ch_20_serviceflag
from
(       
	select orig_facilityid, pointkey, population, orig_channel, orig_serviceflag, interference_free_flag
	from ia.tvsoftware_rep_station_points_canada
) a
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_14_20
	where channel = 14 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) b
on a.orig_facilityid = b.facilityid and a.pointkey = b.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_14_20
	where channel = 15 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) c
on a.orig_facilityid = c.facilityid and a.pointkey = c.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_14_20
	where channel = 16 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) d
on a.orig_facilityid = d.facilityid and a.pointkey = d.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_14_20
	where channel = 17 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) e
on a.orig_facilityid = e.facilityid and a.pointkey = e.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_14_20
	where channel = 18 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) f
on a.orig_facilityid = f.facilityid and a.pointkey = f.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_14_20
	where channel = 19 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) g
on a.orig_facilityid = g.facilityid and a.pointkey = g.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_14_20
	where channel = 20 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) h
on a.orig_facilityid = h.facilityid and a.pointkey = h.pointkey; commit; 


create index tvsoftware_analysis_canada_rep_14_20_orig_facilityid on ia.tvsoftware_analysis_canada_rep_14_20 (orig_facilityid); commit; 
create index tvsoftware_analysis_canada_rep_14_20_pointkey on ia.tvsoftware_analysis_canada_rep_14_20 (pointkey); commit; 
create index tvsoftware_analysis_canada_rep_14_20_orig_channel on ia.tvsoftware_analysis_canada_rep_14_20 (orig_channel); commit; 
create index tvsoftware_analysis_canada_rep_14_20_ch_14 on ia.tvsoftware_analysis_canada_rep_14_20 (ch_14); commit; 
create index tvsoftware_analysis_canada_rep_14_20_ch_15 on ia.tvsoftware_analysis_canada_rep_14_20 (ch_15); commit; 
create index tvsoftware_analysis_canada_rep_14_20_ch_16 on ia.tvsoftware_analysis_canada_rep_14_20 (ch_16); commit; 
create index tvsoftware_analysis_canada_rep_14_20_ch_17 on ia.tvsoftware_analysis_canada_rep_14_20 (ch_17); commit; 
create index tvsoftware_analysis_canada_rep_14_20_ch_18 on ia.tvsoftware_analysis_canada_rep_14_20 (ch_18); commit; 
create index tvsoftware_analysis_canada_rep_14_20_ch_19 on ia.tvsoftware_analysis_canada_rep_14_20 (ch_19); commit; 
create index tvsoftware_analysis_canada_rep_14_20_ch_20 on ia.tvsoftware_analysis_canada_rep_14_20 (ch_20); commit; 
create index tvsoftware_analysis_canada_rep_14_20_interference_free_flag on ia.tvsoftware_analysis_canada_rep_14_20 (interference_free_flag); commit;

--------------------------------------------------------------------------------------------------------
--------STEP 3:  CALCULATE PAIRWISE INTERFERENCE PERCENTAGES FOR ALL STATIONS ON CHANNELS 14-20--------- 
--------------------------------------------------------------------------------------------------------
drop table if exists ia.tvsoftware_pairwise_result_14_20; commit; 
create table ia.tvsoftware_pairwise_result_14_20 
(
orig_facilityid integer,
orig_channel integer,
interferingfacilityid integer,
interferingchannel integer,
interference_population numeric,
interference_free_pop numeric,
interference_pct numeric
); commit;

------>>>>>> US STATIONS - CHANNEL 14

insert into ia.tvsoftware_pairwise_result_14_20

select y.orig_facilityid, ch_14 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_14, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_14_20 a, ia.tvsoftware_interference_14_20 b
        where a.orig_facilityid = b.facilityid and a.ch_14 = b.channel and a.pointkey = b.pointkey and b.channel = 14 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_14, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_14_20
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 15

insert into ia.tvsoftware_pairwise_result_14_20

select y.orig_facilityid, ch_15 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_15, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_14_20 a, ia.tvsoftware_interference_14_20 b
        where a.orig_facilityid = b.facilityid and a.ch_15 = b.channel and a.pointkey = b.pointkey and b.channel = 15 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_15, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_14_20
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 



------>>>>>> US STATIONS - CHANNEL 16

insert into ia.tvsoftware_pairwise_result_14_20

select y.orig_facilityid, ch_16 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_16, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_14_20 a, ia.tvsoftware_interference_14_20 b
        where a.orig_facilityid = b.facilityid and a.ch_16 = b.channel and a.pointkey = b.pointkey and b.channel = 16 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_16, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_14_20
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> US STATIONS - CHANNEL 17

insert into ia.tvsoftware_pairwise_result_14_20

select y.orig_facilityid, ch_17 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_17, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_14_20 a, ia.tvsoftware_interference_14_20 b
        where a.orig_facilityid = b.facilityid and a.ch_17 = b.channel and a.pointkey = b.pointkey and b.channel = 17 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_17, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_14_20
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> US STATIONS - CHANNEL 18

insert into ia.tvsoftware_pairwise_result_14_20

select y.orig_facilityid, ch_18 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_18, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_14_20 a, ia.tvsoftware_interference_14_20 b
        where a.orig_facilityid = b.facilityid and a.ch_18 = b.channel and a.pointkey = b.pointkey and b.channel = 18 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_18, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_14_20
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> US STATIONS - CHANNEL 19

insert into ia.tvsoftware_pairwise_result_14_20

select y.orig_facilityid, ch_19 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_19, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_14_20 a, ia.tvsoftware_interference_14_20 b
        where a.orig_facilityid = b.facilityid and a.ch_19 = b.channel and a.pointkey = b.pointkey and b.channel = 19 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_19, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_14_20
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> US STATIONS - CHANNEL 20

insert into ia.tvsoftware_pairwise_result_14_20

select y.orig_facilityid, ch_20 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_20, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_14_20 a, ia.tvsoftware_interference_14_20 b
        where a.orig_facilityid = b.facilityid and a.ch_20 = b.channel and a.pointkey = b.pointkey and b.channel = 20 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_20, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_14_20
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> CANADIAN STATIONS - CHANNEL 14

insert into ia.tvsoftware_pairwise_result_14_20

select y.orig_facilityid, ch_14 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_14, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_14_20 a, ia.tvsoftware_interference_14_20 b
        where a.orig_facilityid = b.facilityid and a.ch_14 = b.channel and a.pointkey = b.pointkey and b.channel = 14 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_14, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_14_20
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  


------>>>>>> CANADIAN STATIONS - CHANNEL 15

insert into ia.tvsoftware_pairwise_result_14_20

select y.orig_facilityid, ch_15 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_15, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_14_20 a, ia.tvsoftware_interference_14_20 b
        where a.orig_facilityid = b.facilityid and a.ch_15 = b.channel and a.pointkey = b.pointkey and b.channel = 15 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_15, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_14_20
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> CANADIAN STATIONS - CHANNEL 16

insert into ia.tvsoftware_pairwise_result_14_20

select y.orig_facilityid, ch_16 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_16, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_14_20 a, ia.tvsoftware_interference_14_20 b
        where a.orig_facilityid = b.facilityid and a.ch_16 = b.channel and a.pointkey = b.pointkey and b.channel = 16 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_16, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_14_20
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> CANADIAN STATIONS - CHANNEL 17

insert into ia.tvsoftware_pairwise_result_14_20

select y.orig_facilityid, ch_17 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_17, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_14_20 a, ia.tvsoftware_interference_14_20 b
        where a.orig_facilityid = b.facilityid and a.ch_17 = b.channel and a.pointkey = b.pointkey and b.channel = 17 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_17, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_14_20
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> CANADIAN STATIONS - CHANNEL 18

insert into ia.tvsoftware_pairwise_result_14_20

select y.orig_facilityid, ch_18 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_18, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_14_20 a, ia.tvsoftware_interference_14_20 b
        where a.orig_facilityid = b.facilityid and a.ch_18 = b.channel and a.pointkey = b.pointkey and b.channel = 18 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_18, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_14_20
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> CANADIAN STATIONS - CHANNEL 19

insert into ia.tvsoftware_pairwise_result_14_20

select y.orig_facilityid, ch_19 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_19, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_14_20 a, ia.tvsoftware_interference_14_20 b
        where a.orig_facilityid = b.facilityid and a.ch_19 = b.channel and a.pointkey = b.pointkey and b.channel = 19 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_19, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_14_20
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 


------>>>>>> CANADIAN STATIONS - CHANNEL 20

insert into ia.tvsoftware_pairwise_result_14_20

select y.orig_facilityid, ch_20 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_20, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_14_20 a, ia.tvsoftware_interference_14_20 b
        where a.orig_facilityid = b.facilityid and a.ch_20 = b.channel and a.pointkey = b.pointkey and b.channel = 20 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_20, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_14_20
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 
