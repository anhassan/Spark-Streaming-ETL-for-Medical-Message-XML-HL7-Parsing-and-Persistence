# Databricks notebook source
dbutils.fs.rm('dbfs:/mnt/datalake/curated/epic_rltm/encntr_visit_rsn_1',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists epic_rltm.encntr_visit_rsn_1;
# MAGIC CREATE TABLE epic_rltm.encntr_visit_rsn_1  
# MAGIC (    msg_src string,
# MAGIC      msg_tm timestamp,
# MAGIC 	 pat_enc_csn_id long,
# MAGIC 	 encntr_rsn string,
# MAGIC      row_updt_tsp  timestamp,
# MAGIC      insert_user_id string,
# MAGIC      update_user_id string,
# MAGIC      row_insert_tsp timestamp generated always as (now())
# MAGIC   )
# MAGIC USING delta
# MAGIC LOCATION 'dbfs:/mnt/datalake/curated/epic_rltm/encntr_visit_rsn_1';