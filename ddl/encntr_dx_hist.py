# Databricks notebook source
dbutils.fs.rm('dbfs:/mnt/datalake/curated/epic_rltm/encntr_dx_hist',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists epic_rltm.encntr_dx_hist;
# MAGIC create table epic_rltm.encntr_dx_hist
# MAGIC (   msg_src string,
# MAGIC     msg_tm timestamp,
# MAGIC 	pat_enc_csn_id long,
# MAGIC 	dx_icd_cd string,
# MAGIC 	dx_name string,
# MAGIC 	dx_code_type string,
# MAGIC     row_insert_tsp timestamp,
# MAGIC     insert_user_id string
# MAGIC   )
# MAGIC USING delta
# MAGIC LOCATION 'dbfs:/mnt/datalake/curated/epic_rltm/encntr_dx_hist';

# COMMAND ----------

