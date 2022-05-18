# Databricks notebook source
dbutils.fs.rm('dbfs:/mnt/datalake/curated/epic_rltm/adt_dtl_1',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists epic_rltm.adt_dtl_1;
# MAGIC create table epic_rltm.adt_dtl_1(
# MAGIC         msg_typ string,
# MAGIC         msg_src string  ,
# MAGIC         trigger_evnt string ,
# MAGIC         msg_tm timestamp  ,
# MAGIC         msg_nm string ,
# MAGIC         bod_id string ,
# MAGIC         pat_mrn_id string ,
# MAGIC         pat_enc_csn_id long    ,
# MAGIC         birth_date timestamp ,
# MAGIC         death_date timestamp ,
# MAGIC         pat_class_abbr string  ,
# MAGIC         hosp_admsn_time timestamp,
# MAGIC         hosp_disch_time timestamp ,
# MAGIC         department_abbr string ,
# MAGIC         loc_abbr string ,
# MAGIC         room_nm string ,
# MAGIC         bed_label string,
# MAGIC         bed_status string ,
# MAGIC         sex_abbr string ,
# MAGIC         means_of_arrv_abbr string ,
# MAGIC         acuity_level_abbr string,
# MAGIC         ed_disposition_abbr string,
# MAGIC         disch_disp_abbr string ,
# MAGIC         adt_arrival_time timestamp,
# MAGIC         hsp_account_id long,
# MAGIC         accommodation_abbr string,
# MAGIC         user_id string,
# MAGIC         row_insert_tsp timestamp generated always as (now()),
# MAGIC         row_updt_tsp timestamp ,
# MAGIC         insert_user_id string,
# MAGIC         update_user_id string
# MAGIC   )
# MAGIC   USING DELTA
# MAGIC   LOCATION 'dbfs:/mnt/datalake/curated/epic_rltm/adt_dtl_1';

# COMMAND ----------

