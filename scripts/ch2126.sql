------------------------------------------------------------------------------------------------------------
----AUTHOR:  FEDERAL COMMUNICATIONS COMMISSION (FCC)                                                    ---- 
----LAST MODIFIED:  OCTOBER 31, 2014                                                                    ----
------------------------------------------------------------------------------------------------------------

---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
--------- ******* PROCESSING OF CHANNELS 21-26 ******* ---------
---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
---------------------------------------------------------------------------------------------------
-------STEP 1:  LOAD SERVICE AND INTERFERENCE POINT DATA FROM CHANNEL 21-26 DATA FILES-------
---------------------------------------------------------------------------------------------------

-----THIS CREATES TABLE INTO WHICH WE LOAD THE SERVICE.CSV FILE FROM THE CHANNEL 21-26 RUN-----
drop table if exists ia.tvsoftware_service_21_26; commit; 
create table ia.tvsoftware_service_21_26
(
pointkey integer,
facilityid integer,
channel integer,
serviceFlag integer
); commit;


-----THIS CREATES TABLE INTO WHICH WE LOAD THE FIRST INTERFERENCE.CSV FILE-----
drop table if exists ia.tvsoftware_interference_21_26; commit; 
create table ia.tvsoftware_interference_21_26
(
pointKey integer,
facilityID integer,
channel integer,
interferingFacilityID integer,
interferingChannel integer
); commit;

--->>>>> COPIES SERVICE.CSV INTO TABLE <<<<<---
\copy ia.tvsoftware_service_21_26 from 'tvstudyprocessing/ch2126/service.csv' delimiter ',' csv; commit; 

--->>>>> COPIES INTERFERENCE.CSV INTO TABLE <<<<<---
\copy ia.tvsoftware_interference_21_26 from 'tvstudyprocessing/ch2126/interference.csv' delimiter ',' csv; commit; 

--->>>>> CREATES INDEXES ON FREQUENTLY USED COLUMNS <<<<<---
create index tvsoftware_service_21_26_pointkey on ia.tvsoftware_service_21_26 (pointkey); commit; 
create index tvsoftware_service_21_26_channel on ia.tvsoftware_service_21_26 (channel); commit; 
create index tvsoftware_service_21_26_facilityid on ia.tvsoftware_service_21_26 (facilityid); commit; 
create index tvsoftware_interference_21_26_pointkey on ia.tvsoftware_interference_21_26 (pointkey); commit; 
create index tvsoftware_interference_21_26_channel on ia.tvsoftware_interference_21_26 (channel); commit; 
create index tvsoftware_interference_21_26_facilityid on ia.tvsoftware_interference_21_26 (facilityid); commit;


------------------------------------------------------------------------------------------------------------------------------
--------STEP 2:  CREATE SINGLE TABLE THAT IDENTIFIES SERVICE POINTS OF ALL STATIONS REPLICATED ONTO CHANNELS 21 - 26 --------- 
------------------------------------------------------------------------------------------------------------------------------

--->>>>> FOR US STATIONS, CREATES STAGING TABLE OF SERVICE POINTS BASED ON BASELINE POINTS <<<<<---
drop table if exists ia.tvsoftware_analysis_orig_rep_21_26; commit;  
create table ia.tvsoftware_analysis_orig_rep_21_26
(
orig_facilityid integer,
pointkey integer,
population numeric, 
orig_channel integer,
orig_serviceflag integer,
interference_free_flag integer,
ch_21 integer,
ch_21_serviceflag integer,
ch_22 integer,
ch_22_serviceflag integer,
ch_23 integer,
ch_23_serviceflag integer,
ch_24 integer,
ch_24_serviceflag integer,
ch_25 integer,
ch_25_serviceflag integer,
ch_26 integer,
ch_26_serviceflag integer
); commit;


insert into ia.tvsoftware_analysis_orig_rep_21_26

select a.orig_facilityid, a.pointkey, population, orig_channel, orig_serviceflag, interference_free_flag,
b.channel as ch_21, b.serviceflag as ch_21_serviceflag,
c.channel as ch_22, c.serviceflag as ch_22_serviceflag, 
d.channel as ch_23, d.serviceflag as ch_23_serviceflag,
e.channel as ch_24, e.serviceflag as ch_24_serviceflag,
f.channel as ch_25, f.serviceflag as ch_25_serviceflag,
g.channel as ch_26, g.serviceflag as ch_26_serviceflag
from
(       
	select orig_facilityid, pointkey, population, orig_channel, orig_serviceflag, interference_free_flag
	from ia.tvsoftware_rep_station_points
) a
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_21_26
	where channel = 21 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) b
on a.orig_facilityid = b.facilityid and a.pointkey = b.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_21_26
	where channel = 22 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) c
on a.orig_facilityid = c.facilityid and a.pointkey = c.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_21_26
	where channel = 23 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) d
on a.orig_facilityid = d.facilityid and a.pointkey = d.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_21_26
	where channel = 24 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) e
on a.orig_facilityid = e.facilityid and a.pointkey = e.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_21_26
	where channel = 25 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) f
on a.orig_facilityid = f.facilityid and a.pointkey = f.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_21_26
	where channel = 26 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) g
on a.orig_facilityid = g.facilityid and a.pointkey = g.pointkey; commit; 


create index tvsoftware_analysis_orig_rep_21_26_orig_facilityid on ia.tvsoftware_analysis_orig_rep_21_26 (orig_facilityid); commit; 
create index tvsoftware_analysis_orig_rep_21_26_pointkey on ia.tvsoftware_analysis_orig_rep_21_26 (pointkey); commit; 
create index tvsoftware_analysis_orig_rep_21_26_orig_channel on ia.tvsoftware_analysis_orig_rep_21_26 (orig_channel); commit; 
create index tvsoftware_analysis_orig_rep_21_26_ch_21 on ia.tvsoftware_analysis_orig_rep_21_26 (ch_21); commit; 
create index tvsoftware_analysis_orig_rep_21_26_ch_22 on ia.tvsoftware_analysis_orig_rep_21_26 (ch_22); commit; 
create index tvsoftware_analysis_orig_rep_21_26_ch_23 on ia.tvsoftware_analysis_orig_rep_21_26 (ch_23); commit; 
create index tvsoftware_analysis_orig_rep_21_26_ch_24 on ia.tvsoftware_analysis_orig_rep_21_26 (ch_24); commit; 
create index tvsoftware_analysis_orig_rep_21_26_ch_25 on ia.tvsoftware_analysis_orig_rep_21_26 (ch_25); commit; 
create index tvsoftware_analysis_orig_rep_21_26_ch_26 on ia.tvsoftware_analysis_orig_rep_21_26 (ch_26); commit; 
create index tvsoftware_analysis_orig_rep_21_26_interference_free_flag on ia.tvsoftware_analysis_orig_rep_21_26 (interference_free_flag); commit;


--->>>>> FOR CANADIAN STATIONS, CREATES STAGING TABLE OF SERVICE POINTS BASED ON BASELINE POINTS <<<<<---
drop table if exists ia.tvsoftware_analysis_canada_rep_21_26; commit; 
create table ia.tvsoftware_analysis_canada_rep_21_26
(
orig_facilityid integer,
pointkey integer,
population numeric, 
orig_channel integer,
orig_serviceflag integer,
interference_free_flag integer,
ch_21 integer,
ch_21_serviceflag integer,
ch_22 integer,
ch_22_serviceflag integer,
ch_23 integer,
ch_23_serviceflag integer,
ch_24 integer,
ch_24_serviceflag integer,
ch_25 integer,
ch_25_serviceflag integer,
ch_26 integer,
ch_26_serviceflag integer
); commit;


insert into ia.tvsoftware_analysis_canada_rep_21_26

select a.orig_facilityid, a.pointkey, population, orig_channel, orig_serviceflag, interference_free_flag,
b.channel as ch_21, b.serviceflag as ch_21_serviceflag,
c.channel as ch_22, c.serviceflag as ch_22_serviceflag, 
d.channel as ch_23, d.serviceflag as ch_23_serviceflag,
e.channel as ch_24, e.serviceflag as ch_24_serviceflag,
f.channel as ch_25, f.serviceflag as ch_25_serviceflag,
g.channel as ch_26, g.serviceflag as ch_26_serviceflag
from
(       
	select orig_facilityid, pointkey, population, orig_channel, orig_serviceflag, interference_free_flag
	from ia.tvsoftware_rep_station_points_canada
) a
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_21_26
	where channel = 21 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) b
on a.orig_facilityid = b.facilityid and a.pointkey = b.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_21_26
	where channel = 22 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) c
on a.orig_facilityid = c.facilityid and a.pointkey = c.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_21_26
	where channel = 23 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA')
) d
on a.orig_facilityid = d.facilityid and a.pointkey = d.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_21_26
	where channel = 24 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) e
on a.orig_facilityid = e.facilityid and a.pointkey = e.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_21_26
	where channel = 25 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) f
on a.orig_facilityid = f.facilityid and a.pointkey = f.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_21_26
	where channel = 26 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) g
on a.orig_facilityid = g.facilityid and a.pointkey = g.pointkey; commit; 


create index tvsoftware_analysis_canada_rep_21_26_orig_facilityid on ia.tvsoftware_analysis_canada_rep_21_26 (orig_facilityid); commit; 
create index tvsoftware_analysis_canada_rep_21_26_pointkey on ia.tvsoftware_analysis_canada_rep_21_26 (pointkey); commit; 
create index tvsoftware_analysis_canada_rep_21_26_orig_channel on ia.tvsoftware_analysis_canada_rep_21_26 (orig_channel); commit; 
create index tvsoftware_analysis_canada_rep_21_26_ch_21 on ia.tvsoftware_analysis_canada_rep_21_26 (ch_21); commit; 
create index tvsoftware_analysis_canada_rep_21_26_ch_22 on ia.tvsoftware_analysis_canada_rep_21_26 (ch_22); commit; 
create index tvsoftware_analysis_canada_rep_21_26_ch_23 on ia.tvsoftware_analysis_canada_rep_21_26 (ch_23); commit; 
create index tvsoftware_analysis_canada_rep_21_26_ch_24 on ia.tvsoftware_analysis_canada_rep_21_26 (ch_24); commit; 
create index tvsoftware_analysis_canada_rep_21_26_ch_25 on ia.tvsoftware_analysis_canada_rep_21_26 (ch_25); commit; 
create index tvsoftware_analysis_canada_rep_21_26_ch_26 on ia.tvsoftware_analysis_canada_rep_21_26 (ch_26); commit; 
create index tvsoftware_analysis_canada_rep_21_26_interference_free_flag on ia.tvsoftware_analysis_canada_rep_21_26 (interference_free_flag); commit;

--------------------------------------------------------------------------------------------------------
--------STEP 3:  CALCULATE PAIRWISE INTERFERENCE PERCENTAGES FOR ALL STATIONS ON CHANNELS 21-26--------- 
--------------------------------------------------------------------------------------------------------
drop table if exists ia.tvsoftware_pairwise_result_21_26; commit; 
create table ia.tvsoftware_pairwise_result_21_26
(
orig_facilityid integer,
orig_channel integer,
interferingfacilityid integer,
interferingchannel integer,
interference_population numeric,
interference_free_pop numeric,
interference_pct numeric
); commit;


------>>>>>> US STATIONS - CHANNEL 21

insert into ia.tvsoftware_pairwise_result_21_26

select y.orig_facilityid, ch_21 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_21, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_21_26 a, ia.tvsoftware_interference_21_26 b
        where a.orig_facilityid = b.facilityid and a.ch_21 = b.channel and a.pointkey = b.pointkey and b.channel = 21 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_21, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_21_26
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 



------>>>>>> US STATIONS - CHANNEL 22

insert into ia.tvsoftware_pairwise_result_21_26

select y.orig_facilityid, ch_22 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_22, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_21_26 a, ia.tvsoftware_interference_21_26 b
        where a.orig_facilityid = b.facilityid and a.ch_22 = b.channel and a.pointkey = b.pointkey and b.channel = 22 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_22, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_21_26
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 



------>>>>>> US STATIONS - CHANNEL 23

insert into ia.tvsoftware_pairwise_result_21_26

select y.orig_facilityid, ch_23 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_23, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_21_26 a, ia.tvsoftware_interference_21_26 b
        where a.orig_facilityid = b.facilityid and a.ch_23 = b.channel and a.pointkey = b.pointkey and b.channel = 23 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_23, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_21_26
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 24

insert into ia.tvsoftware_pairwise_result_21_26

select y.orig_facilityid, ch_24 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_24, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_21_26 a, ia.tvsoftware_interference_21_26 b
        where a.orig_facilityid = b.facilityid and a.ch_24 = b.channel and a.pointkey = b.pointkey and b.channel = 24 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_24, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_21_26
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 25

insert into ia.tvsoftware_pairwise_result_21_26

select y.orig_facilityid, ch_25 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_25, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_21_26 a, ia.tvsoftware_interference_21_26 b
        where a.orig_facilityid = b.facilityid and a.ch_25 = b.channel and a.pointkey = b.pointkey and b.channel = 25 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_25, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_21_26
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 26

insert into ia.tvsoftware_pairwise_result_21_26

select y.orig_facilityid, ch_26 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_26, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_21_26 a, ia.tvsoftware_interference_21_26 b
        where a.orig_facilityid = b.facilityid and a.ch_26 = b.channel and a.pointkey = b.pointkey and b.channel = 26 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_26, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_21_26
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  


------>>>>>> CANADIAN STATIONS - CHANNEL 21

insert into ia.tvsoftware_pairwise_result_21_26

select y.orig_facilityid, ch_21 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_21, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_21_26 a, ia.tvsoftware_interference_21_26 b
        where a.orig_facilityid = b.facilityid and a.ch_21 = b.channel and a.pointkey = b.pointkey and b.channel = 21 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_21, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_21_26
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 



------>>>>>> CANADIAN STATIONS - CHANNEL 22

insert into ia.tvsoftware_pairwise_result_21_26

select y.orig_facilityid, ch_22 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_22, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_21_26 a, ia.tvsoftware_interference_21_26 b
        where a.orig_facilityid = b.facilityid and a.ch_22 = b.channel and a.pointkey = b.pointkey and b.channel = 22 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_22, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_21_26
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 



------>>>>>> CANADIAN STATIONS - CHANNEL 23

insert into ia.tvsoftware_pairwise_result_21_26

select y.orig_facilityid, ch_23 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_23, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_21_26 a, ia.tvsoftware_interference_21_26 b
        where a.orig_facilityid = b.facilityid and a.ch_23 = b.channel and a.pointkey = b.pointkey and b.channel = 23 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_23, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_21_26
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> CANADIAN STATIONS - CHANNEL 24

insert into ia.tvsoftware_pairwise_result_21_26

select y.orig_facilityid, ch_24 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_24, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_21_26 a, ia.tvsoftware_interference_21_26 b
        where a.orig_facilityid = b.facilityid and a.ch_24 = b.channel and a.pointkey = b.pointkey and b.channel = 24 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_24, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_21_26
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> CANADIAN STATIONS - CHANNEL 25

insert into ia.tvsoftware_pairwise_result_21_26

select y.orig_facilityid, ch_25 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_25, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_21_26 a, ia.tvsoftware_interference_21_26 b
        where a.orig_facilityid = b.facilityid and a.ch_25 = b.channel and a.pointkey = b.pointkey and b.channel = 25 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_25, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_21_26
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> CANADIAN STATIONS - CHANNEL 26

insert into ia.tvsoftware_pairwise_result_21_26

select y.orig_facilityid, ch_26 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_26, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_21_26 a, ia.tvsoftware_interference_21_26 b
        where a.orig_facilityid = b.facilityid and a.ch_26 = b.channel and a.pointkey = b.pointkey and b.channel = 26 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_26, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_21_26
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  
