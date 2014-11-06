------------------------------------------------------------------------------------------------------------
----AUTHOR:  FEDERAL COMMUNICATIONS COMMISSION (FCC)                                                    ---- 
----LAST MODIFIED:  OCTOBER 31, 2014                                                                    ----
------------------------------------------------------------------------------------------------------------

---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
--------- ******* PROCESSING OF CHANNELS 45-51 ******* ---------
---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
---------------------------------------------------------------------------------------------------
-------STEP 1:  LOAD SERVICE AND INTERFERENCE POINT DATA FROM CHANNEL 45-51 DATA FILES-------
---------------------------------------------------------------------------------------------------

-----THIS CREATES TABLE INTO WHICH WE LOAD THE SERVICE.CSV FILE FROM THE CHANNEL 45-51 RUN-----
drop table if exists ia.tvsoftware_service_45_51; commit;  
create table ia.tvsoftware_service_45_51
(
pointkey integer,
facilityid integer,
channel integer,
serviceFlag integer
); commit;


-----THIS CREATES TABLE INTO WHICH WE LOAD THE FIRST INTERFERENCE.CSV FILE-----
drop table if exists ia.tvsoftware_interference_45_51; commit; 
create table ia.tvsoftware_interference_45_51
(
pointKey integer,
facilityID integer,
channel integer,
interferingFacilityID integer,
interferingChannel integer
); commit;

--->>>>> COPIES SERVICE.CSV INTO TABLE <<<<<---
\copy ia.tvsoftware_service_45_51 from 'tvstudyprocessing/ch4551/service.csv' delimiter ',' csv; commit; 

--->>>>> COPIES INTERFERENCE.CSV INTO TABLE <<<<<---
\copy ia.tvsoftware_interference_45_51 from 'tvstudyprocessing/ch4551/interference.csv' delimiter ',' csv; commit; 

--->>>>> CREATES INDEXES ON FREQUENTLY USED COLUMNS <<<<<---
create index tvsoftware_service_45_51_pointkey on ia.tvsoftware_service_45_51 (pointkey); commit; 
create index tvsoftware_service_45_51_channel on ia.tvsoftware_service_45_51 (channel); commit; 
create index tvsoftware_service_45_51_facilityid on ia.tvsoftware_service_45_51 (facilityid); commit; 
create index tvsoftware_interference_45_51_pointkey on ia.tvsoftware_interference_45_51 (pointkey); commit; 
create index tvsoftware_interference_45_51_channel on ia.tvsoftware_interference_45_51 (channel); commit; 
create index tvsoftware_interference_45_51_facilityid on ia.tvsoftware_interference_45_51 (facilityid); commit;


------------------------------------------------------------------------------------------------------------------------------
--------STEP 2:  CREATE SINGLE TABLE THAT IDENTIFIES SERVICE POINTS OF ALL STATIONS REPLICATED ONTO CHANNELS 45 - 51 --------- 
------------------------------------------------------------------------------------------------------------------------------

--->>>>> FOR US STATIONS, CREATES STAGING TABLE OF SERVICE POINTS BASED ON BASELINE POINTS <<<<<---
drop table if exists ia.tvsoftware_analysis_orig_rep_45_51; commit; 
create table ia.tvsoftware_analysis_orig_rep_45_51
(
orig_facilityid integer,
pointkey integer,
population numeric, 
orig_channel integer,
orig_serviceflag integer,
interference_free_flag integer,
ch_45 integer,
ch_45_serviceflag integer,
ch_46 integer,
ch_46_serviceflag integer,
ch_47 integer,
ch_47_serviceflag integer,
ch_48 integer,
ch_48_serviceflag integer,
ch_49 integer,
ch_49_serviceflag integer,
ch_50 integer,
ch_50_serviceflag integer,
ch_51 integer,
ch_51_serviceflag integer
); commit;


insert into ia.tvsoftware_analysis_orig_rep_45_51

select a.orig_facilityid, a.pointkey, population, orig_channel, orig_serviceflag, interference_free_flag,
b.channel as ch_45, b.serviceflag as ch_45_serviceflag,
c.channel as ch_46, c.serviceflag as ch_46_serviceflag, 
d.channel as ch_47, d.serviceflag as ch_47_serviceflag,
e.channel as ch_48, e.serviceflag as ch_48_serviceflag,
f.channel as ch_49, f.serviceflag as ch_49_serviceflag,
g.channel as ch_50, g.serviceflag as ch_50_serviceflag,
h.channel as ch_51, h.serviceflag as ch_51_serviceflag
from
(       
	select orig_facilityid, pointkey, population, orig_channel, orig_serviceflag, interference_free_flag
	from ia.tvsoftware_rep_station_points
) a
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_45_51
	where channel = 45 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) b
on a.orig_facilityid = b.facilityid and a.pointkey = b.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_45_51
	where channel = 46 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) c
on a.orig_facilityid = c.facilityid and a.pointkey = c.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_45_51
	where channel = 47 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) d
on a.orig_facilityid = d.facilityid and a.pointkey = d.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_45_51
	where channel = 48 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) e
on a.orig_facilityid = e.facilityid and a.pointkey = e.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_45_51
	where channel = 49 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) f
on a.orig_facilityid = f.facilityid and a.pointkey = f.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_45_51
	where channel = 50 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) g
on a.orig_facilityid = g.facilityid and a.pointkey = g.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_45_51
	where channel = 51 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
) h
on a.orig_facilityid = h.facilityid and a.pointkey = h.pointkey; commit; 


create index tvsoftware_analysis_orig_rep_45_51_orig_facilityid on ia.tvsoftware_analysis_orig_rep_45_51 (orig_facilityid); commit; 
create index tvsoftware_analysis_orig_rep_45_51_pointkey on ia.tvsoftware_analysis_orig_rep_45_51 (pointkey); commit; 
create index tvsoftware_analysis_orig_rep_45_51_orig_channel on ia.tvsoftware_analysis_orig_rep_45_51 (orig_channel); commit; 
create index tvsoftware_analysis_orig_rep_45_51_ch_45 on ia.tvsoftware_analysis_orig_rep_45_51 (ch_45); commit; 
create index tvsoftware_analysis_orig_rep_45_51_ch_46 on ia.tvsoftware_analysis_orig_rep_45_51 (ch_46); commit; 
create index tvsoftware_analysis_orig_rep_45_51_ch_47 on ia.tvsoftware_analysis_orig_rep_45_51 (ch_47); commit; 
create index tvsoftware_analysis_orig_rep_45_51_ch_48 on ia.tvsoftware_analysis_orig_rep_45_51 (ch_48); commit; 
create index tvsoftware_analysis_orig_rep_45_51_ch_49 on ia.tvsoftware_analysis_orig_rep_45_51 (ch_49); commit; 
create index tvsoftware_analysis_orig_rep_45_51_ch_50 on ia.tvsoftware_analysis_orig_rep_45_51 (ch_50); commit; 
create index tvsoftware_analysis_orig_rep_45_51_ch_51 on ia.tvsoftware_analysis_orig_rep_45_51 (ch_51); commit; 
create index tvsoftware_analysis_orig_rep_45_51_interference_free_flag on ia.tvsoftware_analysis_orig_rep_45_51 (interference_free_flag); commit;


--->>>>> FOR CANADIAN STATIONS, CREATES STAGING TABLE OF SERVICE POINTS BASED ON BASELINE POINTS <<<<<---
drop table if exists ia.tvsoftware_analysis_canada_rep_45_51; commit; 
create table ia.tvsoftware_analysis_canada_rep_45_51
(
orig_facilityid integer,
pointkey integer,
population numeric, 
orig_channel integer,
orig_serviceflag integer,
interference_free_flag integer,
ch_45 integer,
ch_45_serviceflag integer,
ch_46 integer,
ch_46_serviceflag integer,
ch_47 integer,
ch_47_serviceflag integer,
ch_48 integer,
ch_48_serviceflag integer,
ch_49 integer,
ch_49_serviceflag integer,
ch_50 integer,
ch_50_serviceflag integer,
ch_51 integer,
ch_51_serviceflag integer
); commit;


insert into ia.tvsoftware_analysis_canada_rep_45_51

select a.orig_facilityid, a.pointkey, population, orig_channel, orig_serviceflag, interference_free_flag,
b.channel as ch_45, b.serviceflag as ch_45_serviceflag,
c.channel as ch_46, c.serviceflag as ch_46_serviceflag, 
d.channel as ch_47, d.serviceflag as ch_47_serviceflag,
e.channel as ch_48, e.serviceflag as ch_48_serviceflag,
f.channel as ch_49, f.serviceflag as ch_49_serviceflag,
g.channel as ch_50, g.serviceflag as ch_50_serviceflag,
h.channel as ch_51, h.serviceflag as ch_51_serviceflag
from
(       
	select orig_facilityid, pointkey, population, orig_channel, orig_serviceflag, interference_free_flag
	from ia.tvsoftware_rep_station_points_canada
) a
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_45_51
	where channel = 45 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) b
on a.orig_facilityid = b.facilityid and a.pointkey = b.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_45_51
	where channel = 46 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) c
on a.orig_facilityid = c.facilityid and a.pointkey = c.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_45_51
	where channel = 47 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) d
on a.orig_facilityid = d.facilityid and a.pointkey = d.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_45_51
	where channel = 48 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) e
on a.orig_facilityid = e.facilityid and a.pointkey = e.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_45_51
	where channel = 49 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) f
on a.orig_facilityid = f.facilityid and a.pointkey = f.pointkey 
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_45_51
	where channel = 50 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) g
on a.orig_facilityid = g.facilityid and a.pointkey = g.pointkey
left join
(
	select facilityid, channel, pointkey, serviceflag
	from ia.tvsoftware_service_45_51
	where channel = 51 and facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
) h
on a.orig_facilityid = h.facilityid and a.pointkey = h.pointkey; commit; 


create index tvsoftware_analysis_canada_rep_45_51_orig_facilityid on ia.tvsoftware_analysis_canada_rep_45_51 (orig_facilityid); commit; 
create index tvsoftware_analysis_canada_rep_45_51_pointkey on ia.tvsoftware_analysis_canada_rep_45_51 (pointkey); commit; 
create index tvsoftware_analysis_canada_rep_45_51_orig_channel on ia.tvsoftware_analysis_canada_rep_45_51 (orig_channel); commit; 
create index tvsoftware_analysis_canada_rep_45_51_ch_45 on ia.tvsoftware_analysis_canada_rep_45_51 (ch_45); commit; 
create index tvsoftware_analysis_canada_rep_45_51_ch_46 on ia.tvsoftware_analysis_canada_rep_45_51 (ch_46); commit; 
create index tvsoftware_analysis_canada_rep_45_51_ch_47 on ia.tvsoftware_analysis_canada_rep_45_51 (ch_47); commit; 
create index tvsoftware_analysis_canada_rep_45_51_ch_48 on ia.tvsoftware_analysis_canada_rep_45_51 (ch_48); commit; 
create index tvsoftware_analysis_canada_rep_45_51_ch_49 on ia.tvsoftware_analysis_canada_rep_45_51 (ch_49); commit; 
create index tvsoftware_analysis_canada_rep_45_51_ch_50 on ia.tvsoftware_analysis_canada_rep_45_51 (ch_50); commit; 
create index tvsoftware_analysis_canada_rep_45_51_ch_51 on ia.tvsoftware_analysis_canada_rep_45_51 (ch_51); commit; 
create index tvsoftware_analysis_canada_rep_45_51_interference_free_flag on ia.tvsoftware_analysis_canada_rep_45_51 (interference_free_flag); commit;

--------------------------------------------------------------------------------------------------------
--------STEP 3:  CALCULATE PAIRWISE INTERFERENCE PERCENTAGES FOR ALL STATIONS ON CHANNELS 45-51--------- 
--------------------------------------------------------------------------------------------------------
drop table if exists ia.tvsoftware_pairwise_result_45_51; commit; 
create table ia.tvsoftware_pairwise_result_45_51 
(
orig_facilityid integer,
orig_channel integer,
interferingfacilityid integer,
interferingchannel integer,
interference_population numeric,
interference_free_pop numeric,
interference_pct numeric
); commit;


------>>>>>> US STATIONS - CHANNEL 45

insert into ia.tvsoftware_pairwise_result_45_51

select y.orig_facilityid, ch_45 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_45, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_45_51 a, ia.tvsoftware_interference_45_51 b
        where a.orig_facilityid = b.facilityid and a.ch_45 = b.channel and a.pointkey = b.pointkey and b.channel = 45 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_45, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_45_51
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 46

insert into ia.tvsoftware_pairwise_result_45_51

select y.orig_facilityid, ch_46 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_46, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_45_51 a, ia.tvsoftware_interference_45_51 b
        where a.orig_facilityid = b.facilityid and a.ch_46 = b.channel and a.pointkey = b.pointkey and b.channel = 46 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_46, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_45_51
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 47

insert into ia.tvsoftware_pairwise_result_45_51

select y.orig_facilityid, ch_47 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_47, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_45_51 a, ia.tvsoftware_interference_45_51 b
        where a.orig_facilityid = b.facilityid and a.ch_47 = b.channel and a.pointkey = b.pointkey and b.channel = 47 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_47, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_45_51
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 48

insert into ia.tvsoftware_pairwise_result_45_51

select y.orig_facilityid, ch_48 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_48, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_45_51 a, ia.tvsoftware_interference_45_51 b
        where a.orig_facilityid = b.facilityid and a.ch_48 = b.channel and a.pointkey = b.pointkey and b.channel = 48 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_48, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_45_51
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 49

insert into ia.tvsoftware_pairwise_result_45_51

select y.orig_facilityid, ch_49 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_49, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_45_51 a, ia.tvsoftware_interference_45_51 b
        where a.orig_facilityid = b.facilityid and a.ch_49 = b.channel and a.pointkey = b.pointkey and b.channel = 49 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_49, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_45_51
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> US STATIONS - CHANNEL 50

insert into ia.tvsoftware_pairwise_result_45_51

select y.orig_facilityid, ch_50 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_50, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_45_51 a, ia.tvsoftware_interference_45_51 b
        where a.orig_facilityid = b.facilityid and a.ch_50 = b.channel and a.pointkey = b.pointkey and b.channel = 50 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_50, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_45_51
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 



------>>>>>> US STATIONS - CHANNEL 51

insert into ia.tvsoftware_pairwise_result_45_51

select y.orig_facilityid, ch_51 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_51, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_orig_rep_45_51 a, ia.tvsoftware_interference_45_51 b
        where a.orig_facilityid = b.facilityid and a.ch_51 = b.channel and a.pointkey = b.pointkey and b.channel = 51 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'US') 
        group by orig_facilityid, ch_51, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_orig_rep_45_51
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 



------>>>>>> CANADIAN STATIONS - CHANNEL 45

insert into ia.tvsoftware_pairwise_result_45_51

select y.orig_facilityid, ch_45 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_45, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_45_51 a, ia.tvsoftware_interference_45_51 b
        where a.orig_facilityid = b.facilityid and a.ch_45 = b.channel and a.pointkey = b.pointkey and b.channel = 45 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_45, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_45_51
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> CANADIAN STATIONS - CHANNEL 46

insert into ia.tvsoftware_pairwise_result_45_51

select y.orig_facilityid, ch_46 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_46, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_45_51 a, ia.tvsoftware_interference_45_51 b
        where a.orig_facilityid = b.facilityid and a.ch_46 = b.channel and a.pointkey = b.pointkey and b.channel = 46 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_46, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_45_51
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> CANADIAN STATIONS - CHANNEL 47

insert into ia.tvsoftware_pairwise_result_45_51

select y.orig_facilityid, ch_47 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_47, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_45_51 a, ia.tvsoftware_interference_45_51 b
        where a.orig_facilityid = b.facilityid and a.ch_47 = b.channel and a.pointkey = b.pointkey and b.channel = 47 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_47, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_45_51
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> CANADIAN STATIONS - CHANNEL 48

insert into ia.tvsoftware_pairwise_result_45_51

select y.orig_facilityid, ch_48 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_48, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_45_51 a, ia.tvsoftware_interference_45_51 b
        where a.orig_facilityid = b.facilityid and a.ch_48 = b.channel and a.pointkey = b.pointkey and b.channel = 48 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_48, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_45_51
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> CANADIAN STATIONS - CHANNEL 49

insert into ia.tvsoftware_pairwise_result_45_51

select y.orig_facilityid, ch_49 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_49, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_45_51 a, ia.tvsoftware_interference_45_51 b
        where a.orig_facilityid = b.facilityid and a.ch_49 = b.channel and a.pointkey = b.pointkey and b.channel = 49 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_49, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_45_51
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit;  



------>>>>>> CANADIAN STATIONS - CHANNEL 50

insert into ia.tvsoftware_pairwise_result_45_51

select y.orig_facilityid, ch_50 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_50, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_45_51 a, ia.tvsoftware_interference_45_51 b
        where a.orig_facilityid = b.facilityid and a.ch_50 = b.channel and a.pointkey = b.pointkey and b.channel = 50 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_50, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_45_51
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 



------>>>>>> CANADIAN STATIONS - CHANNEL 51

insert into ia.tvsoftware_pairwise_result_45_51

select y.orig_facilityid, ch_51 as orig_channel, interferingfacilityid, interferingchannel, interference_population, interference_free_pop, 
(case when interference_free_pop > 0 then round((100*interference_population/interference_free_pop), 5) else 0 end) as interference_pct
from 
(
	select orig_facilityid, ch_51, interferingfacilityid, interferingchannel,
	sum (population * (case when interference_free_flag = 1 and interferingchannel is not null then 1 else 0 end)) as interference_population
	from ia.tvsoftware_analysis_canada_rep_45_51 a, ia.tvsoftware_interference_45_51 b
        where a.orig_facilityid = b.facilityid and a.ch_51 = b.channel and a.pointkey = b.pointkey and b.channel = 51 and b.facilityid in (select distinct facilityid from ia.tvsoftware_stations where countrycode = 'CA') 
        group by orig_facilityid, ch_51, interferingfacilityid, interferingchannel
        order by interferingfacilityid, interferingchannel
) y
left join
(
        select orig_facilityid, sum(population) as interference_free_pop
	from ia.tvsoftware_analysis_canada_rep_45_51
	where interference_free_flag = 1
	group by orig_facilityid
) z
on y.orig_facilityid = z.orig_facilityid; commit; 
