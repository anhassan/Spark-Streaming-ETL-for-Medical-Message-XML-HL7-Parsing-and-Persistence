-- Databricks notebook source
/*******************************************************************************
-- PURPOSE/USAGE    : SCRIPT TO INSERT DATA INTO TABLES IN EPIC_RLTM SCHEMA IN DATA BRICKS IDP.
-- PLATFORM         : IDP DATA BRICKS
-- SCHEMA           : EPIC_RLTM
-- CREATED          : 03/18/2022
    	1. Insert into table epic_rltm.adt_hist  
        2. Insert into table epic_rltm.encntr_dx_hist 
        3. Insert into table epic_rltm.encntr_er_complnt_hist
        4. Insert into table epic_rltm.encntr_visit_rsn_hist  
*/

TRUNCATE TABLE epic_rltm.adt_hist ;
TRUNCATE TABLE epic_rltm.encntr_dx_hist ;
TRUNCATE TABLE epic_rltm.encntr_er_complnt_hist ;
TRUNCATE TABLE epic_rltm.encntr_visit_rsn_hist ;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/mnt/datalake/curated/epic_rltm/adt_hist', True)
-- MAGIC dbutils.fs.rm('dbfs:/mnt/datalake/curated/epic_rltm/encntr_dx_hist', True)
-- MAGIC dbutils.fs.rm('dbfs:/mnt/datalake/curated/epic_rltm/encntr_er_complnt_hist', True)
-- MAGIC dbutils.fs.rm('dbfs:/mnt/datalake/curated/epic_rltm/encntr_visit_rsn_hist', True)

-- COMMAND ----------

-- TYPE: SQL script commands
-- DEFINITION:  This is a new notebook to do a one time load into the epic realtime ADT history table epic_rltm.adt_hist
--              This code will load from clarity all the currently admitted patients that do not have the discharge date and also any discharges that happened in the last two days.  

insert into epic_rltm.adt_hist (
with adt_dtl as (select distinct
'ETL'                   as msg_typ,
'STL'                   as msg_src,
'ETL'                   as trigger_evnt,  
current_timestamp       as msg_tm,
'ETL'                   as msg_nm,
'ETL'                   as bod_id,
pat.pat_mrn_id          as pat_mrn_id,               
peh.pat_enc_csn_id      as pat_enc_csn_id,    
pat.birth_date          as birth_date,
pat.death_date          as death_date,
upper(zc_class.abbr)    as pat_class_abbr,	
peh.hosp_admsn_time     as hosp_admsn_time,	
peh.hosp_disch_time     as hosp_disch_time,
dep_loc.department_abbr as department_abbr,
dep_loc.loc_abbr        as loc_abbr,
rom.room_name 			as room_nm,
bed.bed_label           as bed_label,
zc_bed.name 			as bed_status,
zc_sex.abbr 			as sex_abbr,
zc_arriv.name 			as means_of_arrv_abbr,
substr(zc_acuity.abbr,3)as acuity_level_abbr, 
zc_ed_disp.abbr 		as ed_disposition_abbr,
zc_disp.abbr            as disch_disp_abbr,
peh.adt_arrival_time    as adt_arrival_time,
peh.hsp_account_id,
c_curr.effective_time,  
c_curr.user_id,
zc_accomm.abbr          as accommodation_abbr,
 --- TO DETERMINE THE FINAL DEPARTMENT.
row_number() over (partition by peh.pat_enc_csn_id,pat.pat_mrn_id  order by  c_curr.seq_num_in_enc desc) as row_num,
bed.bed_status_c,
bed.census_inclusn_yn,
peh.disch_disp_c,
peh.acuity_level_c,
peh.means_of_arrv_c,
peh.ed_disposition_c,
c_curr.accommodation_c
from            clarity.pat_enc_hsp        peh
inner join      clarity.clarity_adt        c_curr   on peh.pat_enc_csn_id   = c_curr.pat_enc_csn_id
                                                  and CAST(c_curr.effective_time as date) >= to_date('2021-07-01','yyyy-MM-dd')   
                                                  and CAST(c_curr.effective_time as date) <=  (current_date -1)
                                                  and c_curr.event_type_c in (1,2,3,5,6)   ---Admission/ Discharge/Transfer In/Patient Update/Census
                                                  and c_curr.event_subtype_c <> 2          --- Filtering out Canceled Events
inner join      clarity.patient             pat         on pat.pat_id                   = peh.pat_id
inner join      clarity.patient_3           p3          on peh.pat_id                   = p3.pat_id and p3.is_test_pat_yn <> 'Y'    --   Exclude Test patients 
left outer join clarity.zc_pat_class        zc_class    on peh.adt_pat_class_c          = zc_class.adt_pat_class_c
inner join      bi_clarity.mv_dep_loc_sa    dep_loc     on c_curr.department_id         = dep_loc.department_id
inner join      clarity.clarity_rom         rom         on c_curr.room_csn_id           = rom.room_csn_id 
inner join      clarity.clarity_bed         bed         on bed.bed_csn_id               = c_curr.bed_csn_id   
left outer join clarity.zc_disch_disp       zc_disp     on peh.disch_disp_c             = zc_disp.disch_disp_c 
left outer join clarity.zc_acuity_level	    zc_acuity	on zc_acuity.acuity_level_c	    = peh.acuity_level_c	
left outer join clarity.zc_sex 			    zc_sex      on zc_sex.rcpt_mem_sex_c	    = pat.sex_c
left outer join clarity.zc_arriv_means      zc_arriv    on zc_arriv.means_of_arrv_c     = peh.means_of_arrv_c
left outer join clarity.zc_ed_disposition   zc_ed_disp  on zc_ed_disp.ed_disposition_c  = peh.ed_disposition_c
left outer join clarity.zc_bed_status		zc_bed      on zc_bed.bed_status_c          = bed.bed_status_c
left outer join clarity.zc_accommodation	zc_accomm   on  zc_accomm.accommodation_c   = c_curr.accommodation_c
where  (CAST(peh.hosp_admsn_time as date) >= to_date('2021-07-01','yyyy-MM-dd') 
          and (peh.hosp_disch_time is null                                      --- Discharge time not present
                or   CAST(peh.hosp_disch_time as date) >= (current_date -2)      --- Discharge's in the last two days.
               )
        )  
and dep_loc.sa_rpt_grp_six_name like '%MERCY%' --- Limiting only to only 'Mercy' Service Area's
and dep_loc.loc_id   not in (120230,120112)    --- Filtering out Hot Springs And Mercy Hospital Independence
and peh.admit_conf_stat_c <> 3                 --- Filtering out cancelled admissions
and pat.death_date is null                     --- Filtering out any patients that had death date populated
)
    
select distinct
msg_typ,
msg_src,
trigger_evnt,  
msg_tm,
msg_nm,
bod_id,
pat_mrn_id,               
pat_enc_csn_id,    
birth_date,
death_date,
pat_class_abbr,	
hosp_admsn_time,	
hosp_disch_time,
department_abbr,
loc_abbr,
room_nm,
bed_label,
bed_status,
sex_abbr,
means_of_arrv_abbr,
acuity_level_abbr, 
ed_disposition_abbr,
disch_disp_abbr,
adt_arrival_time,
hsp_account_id,
accommodation_abbr,
user_id,
current_timestamp        as row_insert_tsp,
current_user             as insert_user_id
from adt_dtl
where row_num = 1 --- getting only the final bed and room details from previous day
);


-- COMMAND ----------

--- #2
-- TYPE: SQL script commands
-- DEFINITION:  This is a new notebook to load do a one time load into the epic realtime encounter diagnosis history table epic_rltm.encntr_dx_hist
--              This code will load from clarity the encounter er complaints for the CSN's present in the epic_rltm.adt_hist table.

insert into epic_rltm.encntr_dx_hist
select 
            adt.msg_src                     as msg_src,
            adt.msg_tm                      as msg_tm,
            adt.pat_enc_csn_id              as pat_enc_csn_id,
            icd10.code						as dx_icd_cd,
            edg.dx_name             		as dx_name,
            'ICD-10-CM'						as dx_code_type,
            current_timestamp               as row_insert_tsp,
	    current_user                    as insert_user_id
from        epic_rltm.adt_hist       adt
inner join clarity.pat_enc_dx        enc_dx  on adt.pat_enc_csn_id =  enc_dx.pat_enc_csn_id
inner join clarity.edg_current_icd10 icd10  on icd10.dx_id = enc_dx.dx_id
inner join clarity.clarity_edg       edg    on icd10.dx_id = edg.dx_id
;

-- COMMAND ----------

--#3
-- TYPE: SQL script commands
-- DEFINITION:  This is a new notebook to load do a one time load into the epic realtime encounter er complaint history table epic_rltm.encntr_er_complnt_hist
--              This code will load from clarity the encounter er complaints for the CSN's present in the epic_rltm.adt_hist table.

insert into epic_rltm.encntr_er_complnt_hist
  select    distinct
            adt.msg_src                     as  msg_src,
            adt.msg_tm                      as msg_tm,
            adt.pat_enc_csn_id              as pat_enc_csn_id,
            er_com.er_complaint             as er_complaint,
            current_timestamp               as row_insert_tsp,
	    current_user                    as insert_user_id
from       epic_rltm.adt_hist         adt
    inner join clarity.pat_enc_er_complnt er_com    on adt.pat_enc_csn_id = 	er_com.pat_enc_csn_id
;            


-- COMMAND ----------

-- #4
-- TYPE: SQL script commands
-- DEFINITION:  This is a new notebook to load do a one time load into the epic realtime encounter reason history table epic_rltm.encntr_visit_rsn_hist
--              This code will load from clarity the encounter reasons for the CSN's present in the epic_rltm.adt_hist table.

insert into epic_rltm.encntr_visit_rsn_hist
  select    distinct
            adt.msg_src                     as msg_src,
            adt.msg_tm                      as msg_tm,
            adt.pat_enc_csn_id              as pat_enc_csn_id,
            cl_rsn_visit.reason_visit_name  as encntr_rsn,
            current_timestamp               as row_insert_tsp,
	    current_user                    as insert_user_id
 from       epic_rltm.adt_hist        adt
    inner join clarity.pat_enc_rsn_visit rsn_visit    on adt.pat_enc_csn_id = rsn_visit.pat_enc_csn_id
    inner join clarity.cl_rsn_for_visit  cl_rsn_visit on rsn_visit.enc_reason_id = cl_rsn_visit.reason_visit_id
;