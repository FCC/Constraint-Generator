------------------------------------------------------------------------------------------------------------
----AUTHOR:  FEDERAL COMMUNICATIONS COMMISSION (FCC)                                                    ---- 
----LAST MODIFIED:  OCTOBER 31, 2014                                                                    ----
------------------------------------------------------------------------------------------------------------

---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
--------- ******* INITIAL DATA PROCESSING ******* ---------
---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
---------------------------------------------------------------------------
-------STEP 1:  CREATE TABLES INTO WHICH TO LOAD THE TV STATION LIST-------
---------------------------------------------------------------------------
--->>>>> NOTE: EACH OF THE 8 RUNS OF TV STUDY PRODUCES THE SAME STATIONS.CSV FILE.  ONLY ONE OF THOSE STATION.CSV FILES NEEDS TO BE LOADED INTO THE TABLE BELOW <<<<<---

drop table if exists ia.tvsoftware_stations; commit;  
create table ia.tvsoftware_stations
(
facilityid integer,
channel integer, 
desired_indicator integer,
undesired_indicator integer,
servicetypekey varchar(2), 
callsign varchar(10), 
city varchar(25), 
state varchar(2), 
countrycode varchar(2),
status  varchar(10), 
fileNumber varchar(25),
latitude numeric,
longitude numeric
); commit; 

--->>>>> COPIES STATIONS.CSV INTO TABLE <<<<<---
\copy ia.tvsoftware_stations from 'tvstudyprocessing/ch0204/stations.csv' delimiter ',' csv; commit; 

--->>>>> CREATES INDEXES ON FREQUENTLY USED COLUMNS <<<<<---
create index tvsoftware_stations_facilityid on ia.tvsoftware_stations (facilityid); commit; 
create index tvsoftware_stations_channel on ia.tvsoftware_stations (channel); commit; 

-------------------------------------------------------------------------------------------
-------STEP 2:  LOAD SERVICE AND INTERFERENCE POINT DATA FROM CHANNEL 2-4 DATA FILES-------
-------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----NOTE:  EACH OF THE 8 FILE SETS FROM TV STUDY PRODUCES POINT DATA FOR (1) A STATION ON ITS CURRENT CHANNEL, AND (2) REPLICATED CHANNELS.                 -----
-----FOR EXAMPLE, THE CHANNEL 2-4 RUN WOULD PRODUCE SERVICE AND INTERFERENCE DATA FOR EACH STATION ON ITS CURRENT CHANNEL AS WELL AS CHANNEL 2, 3, AND 4     -----
-----IN ORDER TO DETERMINE THE INTERFERENCE FREE POINTS, WE MUST USE THE POINT DATA FOR STATIONS ON THEIR CURRENT CHANNEL.  IN THIS CODE, WE ARBITRARILY     -----
-----CHOOSE THE CHANNEL 2-4 RUN OF TV STUDY (ALTHOUGH WE COULD EXTRACT THIS DATA FROM ANY OF THE 8 TV STUDY RUNS                                             -----
------------------------------------------------------------------------------------------------------------------------------------------------------------------

-----THIS CREATES TABLE INTO WHICH WE LOAD THE SERVICE.CSV FILE FROM THE CHANNEL 2-4 RUN-----
drop table if exists ia.tvsoftware_service_2_4; commit;
create table ia.tvsoftware_service_2_4
(
pointkey integer,
facilityid integer,
channel integer,
serviceFlag integer
); commit;


-----THIS CREATES TABLE INTO WHICH WE LOAD THE FIRST INTERFERENCE.CSV FILE-----
drop table if exists ia.tvsoftware_interference_2_4; commit;
create table ia.tvsoftware_interference_2_4
(
pointKey integer,
facilityID integer,
channel integer,
interferingFacilityID integer,
interferingChannel integer
); commit;

--->>>>> COPIES SERVICE.CSV INTO TABLE <<<<<---
\copy ia.tvsoftware_service_2_4 from 'tvstudyprocessing/ch0204/service.csv' delimiter ',' csv; commit; 

--->>>>> COPIES INTERFERENCE.CSV INTO TABLE <<<<<---
\copy ia.tvsoftware_interference_2_4 from 'tvstudyprocessing/ch0204/interference.csv' delimiter ',' csv; commit; 

--->>>>> CREATES INDEXES ON FREQUENTLY USED COLUMNS <<<<<---
create index tvsoftware_service_2_4_pointkey on ia.tvsoftware_service_2_4 (pointkey); commit; 
create index tvsoftware_service_2_4_channel on ia.tvsoftware_service_2_4 (channel); commit; 
create index tvsoftware_service_2_4_facilityid on ia.tvsoftware_service_2_4 (facilityid); commit; 
create index tvsoftware_interference_2_4_pointkey on ia.tvsoftware_interference_2_4 (pointkey); commit; 
create index tvsoftware_interference_2_4_channel on ia.tvsoftware_interference_2_4 (channel); commit; 
create index tvsoftware_interference_2_4_facilityid on ia.tvsoftware_interference_2_4 (facilityid); commit;



-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------STEP 3:  CREATE SINGLE TABLE THAT IDENTIFIES ORIGINAL SERVICE POINTS FOR EACH US STATION AND POINTS WITH CURRENT INTERFERENCE (USED TO DETERMINE THE BASELINE INTERFERENCE FREE POPULATION OF EACH STATION)----------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists ia.tvsoftware_rep_station_points; commit; 
create table ia.tvsoftware_rep_station_points
(
orig_facilityid integer,
pointkey integer,
population numeric, 
orig_channel integer,
orig_serviceflag integer,
interference_free_flag integer
); commit;


--->>>>> TV STATIONS CURRENTLY NOT ON CHANNEL 2, 3, OR 4 <<<<<---

insert into ia.tvsoftware_rep_station_points

select a.facilityid as orig_facilityid, a.pointkey, population, a.channel as orig_channel, a.serviceflag as orig_serviceflag, 
(case when a.serviceflag = 1 and interference_flag is null then 1 else 0 end) as interference_free_flag
from
(
	/* FOR EACH US STATION, RETURNS COVERAGE POINTS WITHIN US AS WELL AS THE POPULATION OF THOSE POINTS */
	select a.facilityid, b.channel, a.pointkey, serviceflag, population
	from ia.tvsoftware_service_2_4 a, ia.tvsoftware_stations b, ia.tvsoftware_points_global_superset c 
	where a.facilityid = b.facilityid and a.channel = b.channel and a.pointkey = c.pointkey and countrykey = 1 and countrycode = 'US' and servicetypekey not in ('TX') and a.channel not in (2, 3, 4) 
) a
left join
(
	/* FINDS DISTINCT POINTS OF INTERFERENCE FOR EACH STATION BASED ON EACH STATION'S CURRENT CHANNEL ASSIGNMENT */  
	select a.facilityid, a.pointkey, 1 as interference_flag
	from ia.tvsoftware_interference_2_4 a, ia.tvsoftware_stations b 
	where a.facilityid = b.facilityid and a.channel = b.channel and a.channel not in (2, 3, 4) and servicetypekey not in ('TX')
        group by a.facilityid, a.pointkey   
) b
on a.facilityid = b.facilityid  and a.pointkey = b.pointkey; commit;  


--->>>>> TV STATIONS CURRENTLY ON CHANNEL 2 <<<<<---

insert into ia.tvsoftware_rep_station_points

select a.facilityid as orig_facilityid, a.pointkey, population, a.channel as orig_channel, a.serviceflag as orig_serviceflag, 
(case when a.serviceflag = 1 and interference_flag is null then 1 else 0 end) as interference_free_flag
from
(
	/* FOR EACH US STATION, RETURNS COVERAGE POINTS WITHIN US AS WELL AS THE POPULATION OF THOSE POINTS */
	select a.facilityid, b.channel, a.pointkey, serviceflag, population
	from ia.tvsoftware_service_2_4 a, ia.tvsoftware_stations b, ia.tvsoftware_points_global_superset c 
	where a.facilityid = b.facilityid and a.channel = b.channel and a.pointkey = c.pointkey and countrykey = 1 and countrycode = 'US' and servicetypekey not in ('TX') and a.channel = 2
) a
left join
(
	/* FINDS DISTINCT POINTS OF INTERFERENCE FOR EACH STATION BASED ON EACH STATION'S CURRENT CHANNEL ASSIGNMENT */  
	select a.facilityid, a.pointkey, 1 as interference_flag
	from ia.tvsoftware_interference_2_4 a, ia.tvsoftware_stations b 
	where a.interferingfacilityid = b.facilityid and a.interferingchannel = b.channel and a.channel = 2 and servicetypekey not in ('TX')
        group by a.facilityid, a.pointkey    
) b
on a.facilityid = b.facilityid  and a.pointkey = b.pointkey; commit;  


--->>>>> TV STATIONS CURRENTLY ON CHANNEL 3 <<<<<---


insert into ia.tvsoftware_rep_station_points

select a.facilityid as orig_facilityid, a.pointkey, population, a.channel as orig_channel, a.serviceflag as orig_serviceflag, 
(case when a.serviceflag = 1 and interference_flag is null then 1 else 0 end) as interference_free_flag
from
(
	/* FOR EACH US STATION, RETURNS COVERAGE POINTS WITHIN US AS WELL AS THE POPULATION OF THOSE POINTS */
	select a.facilityid, b.channel, a.pointkey, serviceflag, population
	from ia.tvsoftware_service_2_4 a, ia.tvsoftware_stations b, ia.tvsoftware_points_global_superset c 
	where a.facilityid = b.facilityid and a.channel = b.channel and a.pointkey = c.pointkey and countrykey = 1 and countrycode = 'US' and servicetypekey not in ('TX') and a.channel = 3
) a
left join
(
	/* FINDS DISTINCT POINTS OF INTERFERENCE FOR EACH STATION BASED ON EACH STATION'S CURRENT CHANNEL ASSIGNMENT */  
	select a.facilityid, a.pointkey, 1 as interference_flag
	from ia.tvsoftware_interference_2_4 a, ia.tvsoftware_stations b 
	where a.interferingfacilityid = b.facilityid and a.interferingchannel = b.channel and a.channel = 3 and servicetypekey not in ('TX')
        group by a.facilityid, a.pointkey    
) b
on a.facilityid = b.facilityid  and a.pointkey = b.pointkey; commit; 


--->>>>> TV STATIONS CURRENTLY ON CHANNEL 4 <<<<<---

insert into ia.tvsoftware_rep_station_points

select a.facilityid as orig_facilityid, a.pointkey, population, a.channel as orig_channel, a.serviceflag as orig_serviceflag, 
(case when a.serviceflag = 1 and interference_flag is null then 1 else 0 end) as interference_free_flag
from
(
	/* FOR EACH US STATION, RETURNS COVERAGE POINTS WITHIN US AS WELL AS THE POPULATION OF THOSE POINTS */
	select a.facilityid, b.channel, a.pointkey, serviceflag, population
	from ia.tvsoftware_service_2_4 a, ia.tvsoftware_stations b, ia.tvsoftware_points_global_superset c 
	where a.facilityid = b.facilityid and a.channel = b.channel and a.pointkey = c.pointkey and countrykey = 1 and countrycode = 'US' and servicetypekey not in ('TX') and a.channel = 4
) a
left join
(
	/* FINDS DISTINCT POINTS OF INTERFERENCE FOR EACH STATION BASED ON EACH STATION'S CURRENT CHANNEL ASSIGNMENT */  
	select a.facilityid, a.pointkey, 1 as interference_flag
	from ia.tvsoftware_interference_2_4 a, ia.tvsoftware_stations b 
	where a.interferingfacilityid = b.facilityid and a.interferingchannel = b.channel and a.channel = 4 and servicetypekey not in ('TX')
        group by a.facilityid, a.pointkey    
) b
on a.facilityid = b.facilityid  and a.pointkey = b.pointkey; commit; 


--->>>>> CREATES INDEXES ON FREQUENTLY USED COLUMNS <<<<<---
create index tvsoftware_rep_station_points_a on ia.tvsoftware_rep_station_points (orig_facilityid); commit;
create index tvsoftware_rep_station_points_b on ia.tvsoftware_rep_station_points (pointkey); commit;
create index tvsoftware_rep_station_points_c on ia.tvsoftware_rep_station_points (interference_free_flag); commit;


----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------STEP 4:  CREATE SINGLE TABLE THAT IDENTIFIES ORIGINAL SERVICE POINTS FOR EACH CANADIAN STATION AND POINTS WITH CURRENT INTERFERENCE (USED TO DETERMINE THE BASELINE INTERFERENCE FREE POPULATION OF EACH STATION----------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists ia.tvsoftware_rep_station_points_canada; commit; 
create table ia.tvsoftware_rep_station_points_canada
(
orig_facilityid integer,
pointkey integer,
population numeric, 
orig_channel integer,
orig_serviceflag integer,
interference_free_flag integer
); commit;


--->>>>> TV STATIONS CURRENTLY NOT ON CHANNEL 2, 3, OR 4 <<<<<---

insert into ia.tvsoftware_rep_station_points_canada

select a.facilityid as orig_facilityid, a.pointkey, population, a.channel as orig_channel, a.serviceflag as orig_serviceflag, 
(case when a.serviceflag = 1 and interference_flag is null then 1 else 0 end) as interference_free_flag
from
(
	/* FOR EACH CANADIAN STATION, RETURNS COVERAGE POINTS WITHIN CANADA AS WELL AS THE POPULATION OF THOSE POINTS */
	select a.facilityid, b.channel, a.pointkey, serviceflag, population
	from ia.tvsoftware_service_2_4 a, ia.tvsoftware_stations b, ia.tvsoftware_points_global_superset c 
	where a.facilityid = b.facilityid and a.channel = b.channel and a.pointkey = c.pointkey and countrykey = 2 and countrycode = 'CA' and servicetypekey not in ('TX') and a.channel not in (2, 3, 4)  
) a
left join
(
	/* FINDS DISTINCT POINTS OF INTERFERENCE FOR EACH STATION'S COVERAGE POINTS */  
	select a.facilityid, a.pointkey, 1 as interference_flag
	from ia.tvsoftware_interference_2_4 a, ia.tvsoftware_stations b 
	where a.facilityid = b.facilityid and a.channel = b.channel and a.channel not in (2, 3, 4) and servicetypekey not in ('TX') 
	group by a.facilityid, a.pointkey  
) b
on a.facilityid = b.facilityid  and a.pointkey = b.pointkey; COMMIT;  


--->>>>> TV STATIONS CURRENTLY ON CHANNEL 2 <<<<<---

insert into ia.tvsoftware_rep_station_points_canada

select a.facilityid as orig_facilityid, a.pointkey, population, a.channel as orig_channel, a.serviceflag as orig_serviceflag, 
(case when a.serviceflag = 1 and interference_flag is null then 1 else 0 end) as interference_free_flag
from
(
	/* FOR EACH CANADIAN STATION, RETURNS COVERAGE POINTS WITHIN CANADA AS WELL AS THE POPULATION OF THOSE POINTS */
	select a.facilityid, b.channel, a.pointkey, serviceflag, population
	from ia.tvsoftware_service_2_4 a, ia.tvsoftware_stations b, ia.tvsoftware_points_global_superset c 
	where a.facilityid = b.facilityid and a.channel = b.channel and a.pointkey = c.pointkey and countrykey = 2 and countrycode = 'CA' and servicetypekey not in ('TX') and a.channel = 2 
) a
left join
(
	/* FINDS DISTINCT POINTS OF INTERFERENCE FOR EACH STATION'S COVERAGE POINTS */  
	select a.facilityid, a.pointkey, 1 as interference_flag
	from ia.tvsoftware_interference_2_4 a, ia.tvsoftware_stations b 
	where a.interferingfacilityid = b.facilityid and a.interferingchannel = b.channel and a.channel = 2 and servicetypekey not in ('TX') 
	group by a.facilityid, a.pointkey  
) b
on a.facilityid = b.facilityid  and a.pointkey = b.pointkey; COMMIT; 


--->>>>> TV STATIONS CURRENTLY ON CHANNEL 3 <<<<<---

insert into ia.tvsoftware_rep_station_points_canada

select a.facilityid as orig_facilityid, a.pointkey, population, a.channel as orig_channel, a.serviceflag as orig_serviceflag, 
(case when a.serviceflag = 1 and interference_flag is null then 1 else 0 end) as interference_free_flag
from
(
	/* FOR EACH CANADIAN STATION, RETURNS COVERAGE POINTS WITHIN CANADA AS WELL AS THE POPULATION OF THOSE POINTS */
	select a.facilityid, b.channel, a.pointkey, serviceflag, population
	from ia.tvsoftware_service_2_4 a, ia.tvsoftware_stations b, ia.tvsoftware_points_global_superset c 
	where a.facilityid = b.facilityid and a.channel = b.channel and a.pointkey = c.pointkey and countrykey = 2 and countrycode = 'CA' and servicetypekey not in ('TX') and a.channel = 3 
) a
left join
(
	/* FINDS DISTINCT POINTS OF INTERFERENCE FOR EACH STATION'S COVERAGE POINTS */  
	select a.facilityid, a.pointkey, 1 as interference_flag
	from ia.tvsoftware_interference_2_4 a, ia.tvsoftware_stations b 
	where a.interferingfacilityid = b.facilityid and a.interferingchannel = b.channel and a.channel = 3 and servicetypekey not in ('TX')
	group by a.facilityid, a.pointkey  
) b
on a.facilityid = b.facilityid  and a.pointkey = b.pointkey; COMMIT;


--->>>>> TV STATIONS CURRENTLY ON CHANNEL 4 <<<<<---

insert into ia.tvsoftware_rep_station_points_canada

select a.facilityid as orig_facilityid, a.pointkey, population, a.channel as orig_channel, a.serviceflag as orig_serviceflag, 
(case when a.serviceflag = 1 and interference_flag is null then 1 else 0 end) as interference_free_flag
from
(
	/* FOR EACH CANADIAN STATION, RETURNS COVERAGE POINTS WITHIN CANADA AS WELL AS THE POPULATION OF THOSE POINTS */
	select a.facilityid, b.channel, a.pointkey, serviceflag, population
	from ia.tvsoftware_service_2_4 a, ia.tvsoftware_stations b, ia.tvsoftware_points_global_superset c 
	where a.facilityid = b.facilityid and a.channel = b.channel and a.pointkey = c.pointkey and countrykey = 2 and countrycode = 'CA' and servicetypekey not in ('TX') and a.channel = 4 
) a
left join
(
	/* FINDS DISTINCT POINTS OF INTERFERENCE FOR EACH STATION'S COVERAGE POINTS */  
	select a.facilityid, a.pointkey, 1 as interference_flag
	from ia.tvsoftware_interference_2_4 a, ia.tvsoftware_stations b 
	where a.interferingfacilityid = b.facilityid and a.interferingchannel = b.channel and a.channel = 4 and servicetypekey not in ('TX')
	group by a.facilityid, a.pointkey  
) b
on a.facilityid = b.facilityid  and a.pointkey = b.pointkey; COMMIT;


create index tvsoftware_rep_station_points_canada_orig_facilityid on ia.tvsoftware_rep_station_points_canada (orig_facilityid); commit;
create index tvsoftware_rep_station_points_canada_pointkey on ia.tvsoftware_rep_station_points_canada (pointkey); commit;
create index tvsoftware_rep_station_points_canada_orig_channel on ia.tvsoftware_rep_station_points_canada (orig_channel); commit;
create index tvsoftware_rep_station_points_canada_interference_free_flag on ia.tvsoftware_rep_station_points_canada (interference_free_flag); commit;


