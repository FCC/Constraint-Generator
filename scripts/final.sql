------------------------------------------------------------------------------------------------------------
----AUTHOR:  FEDERAL COMMUNICATIONS COMMISSION (FCC)                                                    ---- 
----LAST MODIFIED:  AUGUST 13, 2014                                                                     ----
------------------------------------------------------------------------------------------------------------

---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
--------- ******* FINAL PROCESSING BEFORE GENERATING CONSTRAINT FILES ******* ---------
---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^---
------------------------------------------------------------------------------------
-------STEP 1:  COMBINE INDIVIDUAL PAIRWISE TABLES INTO SINGLE PAIRWISE TABLE-------
------------------------------------------------------------------------------------

drop table if exists ia.tvsoftware_pairwise_result_final; commit; 
create table ia.tvsoftware_pairwise_result_final 
(
orig_facilityid integer,
orig_channel integer,
interferingfacilityid integer,
interferingchannel integer,
interference_population numeric,
interference_free_pop numeric,
interference_pct numeric
); commit;


insert into ia.tvsoftware_pairwise_result_final
select *
from ia.tvsoftware_pairwise_result_2_4; commit; 

insert into ia.tvsoftware_pairwise_result_final
select *
from ia.tvsoftware_pairwise_result_5_6_33_36; commit; 

insert into ia.tvsoftware_pairwise_result_final
select *
from ia.tvsoftware_pairwise_result_7_13; commit; 

insert into ia.tvsoftware_pairwise_result_final
select *
from ia.tvsoftware_pairwise_result_14_20; commit; 

insert into ia.tvsoftware_pairwise_result_final
select *
from ia.tvsoftware_pairwise_result_21_26; commit; 

insert into ia.tvsoftware_pairwise_result_final
select *
from ia.tvsoftware_pairwise_result_27_32; commit; 

insert into ia.tvsoftware_pairwise_result_final
select *
from ia.tvsoftware_pairwise_result_38_44; commit; 

insert into ia.tvsoftware_pairwise_result_final
select *
from ia.tvsoftware_pairwise_result_45_51; commit; 


----->>> CREATE INDEXES <<<-----
create index tvsoftware_pairwise_result_final_orig_facilityid on ia.tvsoftware_pairwise_result_final (orig_facilityid); commit;
create index tvsoftware_pairwise_result_final_interferingfacilityid on ia.tvsoftware_pairwise_result_final (interferingfacilityid); commit;
create index tvsoftware_pairwise_result_final_orig_channel on ia.tvsoftware_pairwise_result_final (orig_channel); commit;
create index tvsoftware_pairwise_result_final_interferingchannel on ia.tvsoftware_pairwise_result_final (interferingchannel); commit;



--------------------------------------------------------------------
-------STEP 2:  ADD FINAL MEXICAN STATIONS INTO STATION TABLE-------
--------------------------------------------------------------------

delete from ia.tvsoftware_stations where countrycode = 'MX'; commit; 

insert into ia.tvsoftware_stations 

select *
from ia.ia_mexican_stations; commit; 


