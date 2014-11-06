------------------------------------------------------------------------------------------------------------
----AUTHOR:  FEDERAL COMMUNICATIONS COMMISSION (FCC)                                                    ---- 
----LAST MODIFIED:  OCTOBER 31, 2014                                                                    ----
----DESCRIPTION:  CODE USED TO LOAD REFERENCE TABLES USED TO GENERATE CONSTRAINTS                       ----
------------------------------------------------------------------------------------------------------------

--->>>> NOTE: THIS IS A ONE TIME TABLE LOAD OF REFERENCE TABLES.  YOU ONLY NEED TO LOAD THIS DATA ONCE (NOT EVERY TIME YOU RUN THE TV STUDY SOFTWARE) <<<<---

------------------------------------------------------------------------
--->>>>>>> STEP 1: CREATE TABLES INTO WHICH TO LOAD THE REFERENCE TABLES
------------------------------------------------------------------------

-----THIS CREATES TABLE INTO WHICH WE LOAD A LIST OF US LAND MOBILE (LM) STATIONS-----
drop table if exists ia.ia_lm_master; commit;
CREATE TABLE ia.ia_lm_master
(
facility_id integer NOT NULL,
fac_callsign varchar(5),
fac_channel integer,
city varchar(100),
stateabbr varchar(2),
lat_deg numeric,
lat_min numeric,
lat_sec numeric,
lon_deg numeric,
lon_min numeric,
lon_sec numeric,
latitude numeric,
longitude numeric,
station_flag varchar(1)
); commit;

--->>>>> COPIES us_lm_stations.csv INTO TABLE <<<<<---
\copy ia.ia_lm_master from 'tvstudyprocessing/reference/us_lm_stations.csv' delimiter ',' csv; commit; 


-----THIS CREATES TABLE INTO WHICH WE LOAD A LIST OF US LAND MOBILE WAIVER (LMW) STATIONS-----
drop table if exists ia.ia_lmw_master; commit;
CREATE TABLE ia.ia_lmw_master

(
facility_id integer NOT NULL,
fac_callsign varchar(10),
fac_channel integer,
fac_service varchar(20),
county varchar(20),
stateabbr varchar(10),
location_number integer,
location_type varchar(20),
lat_deg numeric,
lat_min numeric,
lat_sec numeric,
lat_direction varchar(20),
lon_deg numeric,
lon_min numeric,
lon_sec numeric,
lon_direction varchar(20),
latitude numeric,
longitude numeric,
station_flag varchar(1)
); commit; 

--->>>>> COPIES us_lmw_stations.csv INTO TABLE <<<<<---
\copy ia.ia_lmw_master from 'tvstudyprocessing/reference/us_lmw_stations.csv' delimiter ',' csv; commit;


-----THIS CREATES TABLE INTO WHICH WE LOAD PAIRWISE INTERFERENCE DATA BETWEEN TV AND LM STATIONS-----
drop table if exists ia.ia_lm_lmw_interference_table; commit;
CREATE TABLE ia.ia_lm_lmw_interference_table 

( 
buffer_type varchar(20),
facility_id integer,
prot_channel integer,
prot_facility_id integer,
fac_service varchar(2),
adj_level integer
); commit;

--->>>>> COPIES lm_lmw_interference.csv INTO TABLE <<<<<---
\copy ia.ia_lm_lmw_interference_table from 'tvstudyprocessing/reference/lm_lmw_interference.csv' delimiter ',' csv; commit;


-----THIS CREATES TABLE INTO WHICH WE LOAD PAIRWISE INTERFERENCE DATA BETWEEN MEXICAN AND US TV STATIONS-----
drop table if exists ia.ia_mx_interference_table; commit;
CREATE TABLE ia.ia_mx_interference_table 

(
buffer_type varchar(20),
facility_id integer,
fac_channel integer,
prot_channel integer,
prot_facility_id integer,
fac_service varchar(2),
adj_level integer
); commit; 

--->>>>> COPIES mexico_interference.csv INTO TABLE <<<<<---
\copy ia.ia_mx_interference_table from 'tvstudyprocessing/reference/mexico_interference.csv' delimiter ',' csv; commit;


-----THIS CREATES TABLE INTO WHICH WE LOAD A FULL LIST OF MEXICAN TV STATIONS-----
drop table if exists ia.ia_mexican_stations; commit;
CREATE TABLE ia.ia_mexican_stations
(
  facilityid integer,
  channel integer,
  desired_indicator integer,
  undesired_indicator integer,
  servicetypekey character varying(2),
  callsign character varying(10),
  city character varying(25),
  state character varying(2),
  countrycode character varying(2),
  status character varying(10),
  filenumber character varying(25),
  latitude numeric,
  longitude numeric
); commit; 

--->>>>> COPIES mexican_stations.csv INTO TABLE <<<<<---
\copy ia.ia_mexican_stations from 'tvstudyprocessing/reference/mexican_stations.csv' delimiter ',' csv; commit;


-----THIS LOADS THE GLOBAL GRID OF POINTS USED BY TV STUDY.  ALTHOUGH EACH RUN OF TV STUDY PRODUCES A POINTS FILE, WE LOAD THE GLOBAL SET ONCE, UP FRONT - AVOIDING THE NEED TO LOAD MULTIPLE TIMES-----
drop table if exists ia.tvsoftware_points_global_superset; commit;
CREATE TABLE ia.tvsoftware_points_global_superset
(
  pointkey integer,
  celllatitudeindex integer,
  celllongitudeindex integer,
  countrykey integer,
  latitude numeric,
  longitude numeric,
  area numeric,
  population numeric
); commit;

--->>>>> COPIES global_points.csv INTO TABLE <<<<<---
\copy ia.tvsoftware_points_global_superset from 'tvstudyprocessing/reference/global_points.csv' delimiter ',' csv; commit;


-----------------------------------------------------------
--->>>>>>> STEP 2: CREATE INDEXES ON GLOBAL POINTS TABLES
-----------------------------------------------------------
create index tvsoftware_points_global_superset_pointkey on ia.tvsoftware_points_global_superset (pointkey); commit; 
create index tvsoftware_points_global_superset_countrykey on ia.tvsoftware_points_global_superset (countrykey); commit; 
