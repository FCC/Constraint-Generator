------------------------------------------------------------------------------------------------------------
----AUTHOR:  FEDERAL COMMUNICATIONS COMMISSION (FCC)                                                    ---- 
----LAST MODIFIED:  OCTOBER 31, 2014                                                                    ----
------------------------------------------------------------------------------------------------------------

---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
--------- ******* PROCESSING OF CHANNELS 38-44 ******* ---------
---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
---------------------------------------------------------------------------------------------
-------STEP 1:  LOAD SERVICE AND INTERFERENCE POINT DATA FROM CHANNEL 38-44 DATA FILES-------
---------------------------------------------------------------------------------------------

-----THIS CREATES TABLE INTO WHICH WE LOAD THE SERVICE.CSV FILE FROM THE CHANNEL 38-44 RUN-----
drop table if exists ia.tvsoftware_service_38_44; commit; 
create table ia.tvsoftware_service_38_44
(
pointkey integer,
facilityid integer,
channel integer,
serviceFlag integer
); commit;


-----THIS CREATES TABLE INTO WHICH WE LOAD THE FIRST INTERFERENCE.CSV FILE-----
drop table if exists ia.tvsoftware_interference_38_44; commit; 
create table ia.tvsoftware_interference_38_44
(
pointKey integer,
facilityID integer,
channel integer,
interferingFacilityID integer,
interferingChannel integer
); commit;

--->>>>> COPIES SERVICE.CSV INTO TABLE <<<<<---
\copy ia.tvsoftware_service_38_44 from 'tvstudyprocessing/ch3844/service.csv' delimiter ',' csv; commit; 

--->>>>> COPIES INTERFERENCE.CSV INTO TABLE <<<<<---
\copy ia.tvsoftware_interference_38_44 from 'tvstudyprocessing/ch3844/interference.csv' delimiter ',' csv; commit; 

--->>>>> CREATES INDEXES ON FREQUENTLY USED COLUMNS <<<<<---
create index tvsoftware_service_38_44_pointkey on ia.tvsoftware_service_38_44 (pointkey); commit; 
create index tvsoftware_service_38_44_channel on ia.tvsoftware_service_38_44 (channel); commit; 
create index tvsoftware_service_38_44_facilityid on ia.tvsoftware_service_38_44 (facilityid); commit; 
create index tvsoftware_interference_38_44_pointkey on ia.tvsoftware_interference_38_44 (pointkey); commit; 
create index tvsoftware_interference_38_44_channel on ia.tvsoftware_interference_38_44 (channel); commit; 
create index tvsoftware_interference_38_44_facilityid on ia.tvsoftware_interference_38_44 (facilityid); commit;


------------------------------------------------------------------------------------------------------------------------------
--------STEP 2:  CREATE SINGLE TABLE THAT IDENTIFIES SERVICE POINTS OF ALL STATIONS REPLICATED ONTO CHANNELS 38 - 44 --------- 
------------------------------------------------------------------------------------------------------------------------------

--->>>>> FOR US STATIONS, CREATES STAGING TABLE OF SERVICE POINTS BASED ON BASELINE POINTS <<<<<---
drop table if exists ia.tvsoftware_analysis_orig_rep_38_44; commit;
create table ia.tvsoftware_analysis_orig_rep_38_44
(
orig_facilityid integer,
pointkey integer,
population numeric, 
orig_channel integer,
orig_serviceflag integer,
interference_free_flag integer,
ch_38 integer,
ch_38_serviceflag integer,
ch_39 integer,
ch_39_serviceflag integer,
ch_40 integer,
ch_40_serviceflag integer,
ch_41 integer,
ch_41_serviceflag integer,
ch_42 integer,
ch_42_serviceflag integer,
ch_43 integer,
ch_43_serviceflag integer,
ch_44 integer,
ch_44_serviceflag integer
); commit;


insert into ia.tvsoftware_analysis_orig_rep_38_44

select a.orig_facilityid, a.pointkey, population, orig_channel, orig_serviceflag, interference_free_flag,
b.channel as ch_38, b.serviceflag as ch_38_serviceflag,
c.channel as ch_39, c.serviceflag as ch_39_serviceflag, 
d.channel as ch_40, d.serviceflag as ch_40_serviceflag,
e.channel as ch_41, e.serviceflag as ch_41_serviceflag,
f.channel as ch_42, f.serviceflag as ch_42_serviceflag,
g.channel as ch_43, g.serviceflag as ch_43_serviceflag,
h.channel as ch_44, h.serviceflag as ch_44_serviceflag
from
(       
	select orig_facilityid, pointkey, population, orig_channel, orig_serviceflag, interference_free_flag
	from ia.tvsoftware_rep_station_points
) a
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_38_44
	where channel = 38 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) b
on a.orig_facilityid = b.facilityid and a.pointkey = b.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_38_44
	where channel = 39 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) c
on a.orig_facilityid = c.facilityid and a.pointkey = c.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_38_44
	where channel = 40 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) d
on a.orig_facilityid = d.facilityid and a.pointkey = d.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_38_44
	where channel = 41 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) e
on a.orig_facilityid = e.facilityid and a.pointkey = e.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_38_44
	where channel = 42 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) f
on a.orig_facilityid = f.facilityid and a.pointkey = f.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_38_44
	where channel = 43 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) g
on a.orig_facilityid = g.facilityid and a.pointkey = g.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_38_44
	where channel = 44 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) h
on a.orig_facilityid = h.facilityid and a.pointkey = h.pointkey; commit; 


create index tvsoftware_analysis_orig_rep_38_44_orig_facilityid on ia.tvsoftware_analysis_orig_rep_38_44 (orig_facilityid); commit; 
create index tvsoftware_analysis_orig_rep_38_44_pointkey on ia.tvsoftware_analysis_orig_rep_38_44 (pointkey); commit; 
create index tvsoftware_analysis_orig_rep_38_44_orig_channel on ia.tvsoftware_analysis_orig_rep_38_44 (orig_channel); commit; 
create index tvsoftware_analysis_orig_rep_38_44_ch_38 on ia.tvsoftware_analysis_orig_rep_38_44 (ch_38); commit; 
create index tvsoftware_analysis_orig_rep_38_44_ch_39 on ia.tvsoftware_analysis_orig_rep_38_44 (ch_39); commit; 
create index tvsoftware_analysis_orig_rep_38_44_ch_40 on ia.tvsoftware_analysis_orig_rep_38_44 (ch_40); commit; 
create index tvsoftware_analysis_orig_rep_38_44_ch_41 on ia.tvsoftware_analysis_orig_rep_38_44 (ch_41); commit; 
create index tvsoftware_analysis_orig_rep_38_44_ch_42 on ia.tvsoftware_analysis_orig_rep_38_44 (ch_42); commit; 
create index tvsoftware_analysis_orig_rep_38_44_ch_43 on ia.tvsoftware_analysis_orig_rep_38_44 (ch_43); commit; 
create index tvsoftware_analysis_orig_rep_38_44_ch_44 on ia.tvsoftware_analysis_orig_rep_38_44 (ch_44); commit; 
create index tvsoftware_analysis_orig_rep_38_44_interference_free_flag on ia.tvsoftware_analysis_orig_rep_38_44 (interference_free_flag); commit;

--->>>>> FOR CANADIAN STATIONS, CREATES STAGING TABLE OF SERVICE POINTS BASED ON BASELINE POINTS <<<<<---
drop table if exists ia.tvsoftware_analysis_canada_rep_38_44; commit; 
create table ia.tvsoftware_analysis_canada_rep_38_44
(
orig_facilityid integer,
pointkey integer,
population numeric, 
orig_channel integer,
orig_serviceflag integer,
interference_free_flag integer,
ch_38 integer,
ch_38_serviceflag integer,
ch_39 integer,
ch_39_serviceflag integer,
ch_40 integer,
ch_40_serviceflag integer,
ch_41 integer,
ch_41_serviceflag integer,
ch_42 integer,
ch_42_serviceflag integer,
ch_43 integer,
ch_43_serviceflag integer,
ch_44 integer,
ch_44_serviceflag integer
); commit;


insert into ia.tvsoftware_analysis_canada_rep_38_44

select a.orig_facilityid, a.pointkey, population, orig_channel, orig_serviceflag, interference_free_flag,
b.channel as ch_38, b.serviceflag as ch_38_serviceflag,
c.channel as ch_39, c.serviceflag as ch_39_serviceflag, 
d.channel as ch_40, d.serviceflag as ch_40_serviceflag,
e.channel as ch_41, e.serviceflag as ch_41_serviceflag,
f.channel as ch_42, f.serviceflag as ch_42_serviceflag,
g.channel as ch_43, g.serviceflag as ch_43_serviceflag,
h.channel as ch_44, h.serviceflag as ch_44_serviceflag
from
(       
	select orig_facilityid, pointkey, population, orig_channel, orig_serviceflag, interference_free_flag
	from ia.tvsoftware_rep_station_points_canada
) a
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_38_44
	where channel = 38 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) b
on a.orig_facilityid = b.facilityid and a.pointkey = b.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_38_44
	where channel = 39 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) c
on a.orig_facilityid = c.facilityid and a.pointkey = c.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_38_44
	where channel = 40 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) d
on a.orig_facilityid = d.facilityid and a.pointkey = d.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_38_44
	where channel = 41 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) e
on a.orig_facilityid = e.facilityid and a.pointkey = e.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_38_44
	where channel = 42 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) f
on a.orig_facilityid = f.facilityid and a.pointkey = f.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_38_44
	where channel = 43 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) g
on a.orig_facilityid = g.facilityid and a.pointkey = g.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_38_44
	where channel = 44 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) h
on a.orig_facilityid = h.facilityid and a.pointkey = h.pointkey; commit; 


create index tvsoftware_analysis_canada_rep_38_44_orig_facilityid on ia.tvsoftware_analysis_canada_rep_38_44 (orig_facilityid); commit; 
create index tvsoftware_analysis_canada_rep_38_44_pointkey on ia.tvsoftware_analysis_canada_rep_38_44 (pointkey); commit; 
create index tvsoftware_analysis_canada_rep_38_44_orig_channel on ia.tvsoftware_analysis_canada_rep_38_44 (orig_channel); commit; 
create index tvsoftware_analysis_canada_rep_38_44_ch_38 on ia.tvsoftware_analysis_canada_rep_38_44 (ch_38); commit; 
create index tvsoftware_analysis_canada_rep_38_44_ch_39 on ia.tvsoftware_analysis_canada_rep_38_44 (ch_39); commit; 
create index tvsoftware_analysis_canada_rep_38_44_ch_40 on ia.tvsoftware_analysis_canada_rep_38_44 (ch_40); commit; 
create index tvsoftware_analysis_canada_rep_38_44_ch_41 on ia.tvsoftware_analysis_canada_rep_38_44 (ch_41); commit; 
create index tvsoftware_analysis_canada_rep_38_44_ch_42 on ia.tvsoftware_analysis_canada_rep_38_44 (ch_42); commit; 
create index tvsoftware_analysis_canada_rep_38_44_ch_43 on ia.tvsoftware_analysis_canada_rep_38_44 (ch_43); commit; 
create index tvsoftware_analysis_canada_rep_38_44_ch_44 on ia.tvsoftware_analysis_canada_rep_38_44 (ch_44); commit; 
create index tvsoftware_analysis_canada_rep_38_44_interference_free_flag on ia.tvsoftware_analysis_canada_rep_38_44 (interference_free_flag); commit;


--------------------------------------------------------------------------------------------------------
--------STEP 3:  CALCULATE PAIRWISE INTERFERENCE PERCENTAGES FOR ALL STATIONS ON CHANNELS 38-44--------- 
--------------------------------------------------------------------------------------------------------
drop table if exists ia.tvsoftware_pairwise_result_38_44; commit; 
create table ia.tvsoftware_pairwise_result_38_44 
(
orig_facilityid integer,
orig_channel integer,
interferingfacilityid integer,
interferingchannel integer,
interference_population numeric,
interference_free_pop numeric,
interference_pct numeric
); commit;


------>>>>>> US STATIONS - CHANNEL 38

insert into ia.tvsoftware_pairwise_result_38_44

select y.orig_facilityid, ch_38 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_38, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_38_44 a, ia.tvsoftware_interference_38_44 b
        where a.orig_facilityid = b.facilityid and a.ch_38 = b.channel and a.pointkey = b.pointkey and b.channel = 38 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_38, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_38_44
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 39

insert into ia.tvsoftware_pairwise_result_38_44

select y.orig_facilityid, ch_39 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_39, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_38_44 a, ia.tvsoftware_interference_38_44 b
        where a.orig_facilityid = b.facilityid and a.ch_39 = b.channel and a.pointkey = b.pointkey and b.channel = 39 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_39, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_38_44
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 40

insert into ia.tvsoftware_pairwise_result_38_44

select y.orig_facilityid, ch_40 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_40, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_38_44 a, ia.tvsoftware_interference_38_44 b
        where a.orig_facilityid = b.facilityid and a.ch_40 = b.channel and a.pointkey = b.pointkey and b.channel = 40 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_40, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_38_44
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 41

insert into ia.tvsoftware_pairwise_result_38_44

select y.orig_facilityid, ch_41 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_41, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_38_44 a, ia.tvsoftware_interference_38_44 b
        where a.orig_facilityid = b.facilityid and a.ch_41 = b.channel and a.pointkey = b.pointkey and b.channel = 41 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_41, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_38_44
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 42

insert into ia.tvsoftware_pairwise_result_38_44

select y.orig_facilityid, ch_42 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_42, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_38_44 a, ia.tvsoftware_interference_38_44 b
        where a.orig_facilityid = b.facilityid and a.ch_42 = b.channel and a.pointkey = b.pointkey and b.channel = 42 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_42, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_38_44
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 43

insert into ia.tvsoftware_pairwise_result_38_44

select y.orig_facilityid, ch_43 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_43, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_38_44 a, ia.tvsoftware_interference_38_44 b
        where a.orig_facilityid = b.facilityid and a.ch_43 = b.channel and a.pointkey = b.pointkey and b.channel = 43 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_43, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_38_44
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 44

insert into ia.tvsoftware_pairwise_result_38_44

select y.orig_facilityid, ch_44 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_44, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_38_44 a, ia.tvsoftware_interference_38_44 b
        where a.orig_facilityid = b.facilityid and a.ch_44 = b.channel and a.pointkey = b.pointkey and b.channel = 44 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_44, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_38_44
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> CANADIAN STATIONS - CHANNEL 38

insert into ia.tvsoftware_pairwise_result_38_44

select y.orig_facilityid, ch_38 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_38, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_38_44 a, ia.tvsoftware_interference_38_44 b
        where a.orig_facilityid = b.facilityid and a.ch_38 = b.channel and a.pointkey = b.pointkey and b.channel = 38 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_38, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_38_44
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> CANADIAN STATIONS - CHANNEL 39

insert into ia.tvsoftware_pairwise_result_38_44

select y.orig_facilityid, ch_39 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_39, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_38_44 a, ia.tvsoftware_interference_38_44 b
        where a.orig_facilityid = b.facilityid and a.ch_39 = b.channel and a.pointkey = b.pointkey and b.channel = 39 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_39, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_38_44
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> CANADIAN STATIONS - CHANNEL 40

insert into ia.tvsoftware_pairwise_result_38_44

select y.orig_facilityid, ch_40 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_40, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_38_44 a, ia.tvsoftware_interference_38_44 b
        where a.orig_facilityid = b.facilityid and a.ch_40 = b.channel and a.pointkey = b.pointkey and b.channel = 40 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_40, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_38_44
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> CANADIAN STATIONS - CHANNEL 41

insert into ia.tvsoftware_pairwise_result_38_44

select y.orig_facilityid, ch_41 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_41, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_38_44 a, ia.tvsoftware_interference_38_44 b
        where a.orig_facilityid = b.facilityid and a.ch_41 = b.channel and a.pointkey = b.pointkey and b.channel = 41 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_41, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_38_44
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> CANADIAN STATIONS - CHANNEL 42

insert into ia.tvsoftware_pairwise_result_38_44

select y.orig_facilityid, ch_42 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_42, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_38_44 a, ia.tvsoftware_interference_38_44 b
        where a.orig_facilityid = b.facilityid and a.ch_42 = b.channel and a.pointkey = b.pointkey and b.channel = 42 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_42, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_38_44
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> CANADIAN STATIONS - CHANNEL 43

insert into ia.tvsoftware_pairwise_result_38_44

select y.orig_facilityid, ch_43 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_43, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_38_44 a, ia.tvsoftware_interference_38_44 b
        where a.orig_facilityid = b.facilityid and a.ch_43 = b.channel and a.pointkey = b.pointkey and b.channel = 43 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_43, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_38_44
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> CANADIAN STATIONS - CHANNEL 44

insert into ia.tvsoftware_pairwise_result_38_44

select y.orig_facilityid, ch_44 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_44, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_38_44 a, ia.tvsoftware_interference_38_44 b
        where a.orig_facilityid = b.facilityid and a.ch_44 = b.channel and a.pointkey = b.pointkey and b.channel = 44 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_44, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_38_44
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  
