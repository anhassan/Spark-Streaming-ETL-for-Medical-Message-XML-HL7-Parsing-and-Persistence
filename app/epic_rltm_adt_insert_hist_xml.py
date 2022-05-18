# Databricks notebook source
#%fs rm -r /mnt/datalake/real_time/adt

# COMMAND ----------

directory_path ="/mnt/datalake/real_time/adt"

# COMMAND ----------

#%fs rm -r /mnt/datalake/checkpoints_epic_rltm/test

# COMMAND ----------

# MAGIC %run ../utils/utilities

# COMMAND ----------

# MAGIC %run ../code/xml/adt_dtl_hist

# COMMAND ----------

adt_hist = "epic_rltm.adt_hist"
checkpoint_adt_hist = "/mnt/datalake/checkpoints_epic_rltm/xml/hist/adt_dtl_hist"

# COMMAND ----------

# MAGIC %run ../code/xml/encntr_dx_hist

# COMMAND ----------

encntr_dx_hist ="epic_rltm.encntr_dx_hist"
checkpoint_encntr_dx_hist = "/mnt/datalake/checkpoints_epic_rltm/xml/hist/encntr_dx_hist"

# COMMAND ----------

# MAGIC %run ../code/xml/enctr_visit_rsn_hist

# COMMAND ----------

enctr_visit_rsn_hist ="epic_rltm.encntr_visit_rsn_hist"
checkpoint_enctr_visit_rsn_hist = "/mnt/datalake/checkpoints_epic_rltm/xml/hist/enctr_visit_rsn_hist"

# COMMAND ----------

# MAGIC %run ../code/xml/encntr_er_complnt_hist

# COMMAND ----------

enctr_er_complnt_hist ="epic_rltm.encntr_er_complnt_hist"
checkpoint_enctr_er_complnt_hist = "/mnt/datalake/checkpoints_epic_rltm/xml/hist/enctr_er_complnt_hist"

# COMMAND ----------

# MAGIC %md
# MAGIC ###***Adt Hist Stream***

# COMMAND ----------

adt_dtl_df = invoke_adt_dtl_process(directory_path)
write_to_delta_sorted_with_logs(adt_dtl_df,adt_hist,checkpoint_adt_hist)

# COMMAND ----------

# MAGIC %md
# MAGIC ###***Enctr Dx Hist Stream***

# COMMAND ----------

encntr_dx_df = invoke_encntr_dx_process(directory_path)
write_encntr_dx_delta(encntr_dx_df,encntr_dx_hist,checkpoint_encntr_dx_hist)

# COMMAND ----------

# MAGIC %md
# MAGIC ###***Enctr Visit Rsn Hist Stream***

# COMMAND ----------

enctr_visit_rsn_df = invoke_enctr_visit_rsn_process(directory_path)
write_enctr_visit_rsn_delta(enctr_visit_rsn_df,enctr_visit_rsn_hist,checkpoint_enctr_visit_rsn_hist)

# COMMAND ----------

# MAGIC %md
# MAGIC ###***Enctr Er Complnt Hist Stream***

# COMMAND ----------

enctr_er_complnt_df = invoke_enctr_er_complnt_process(directory_path)
write_enctr_er_complnt_delta(enctr_er_complnt_df,enctr_er_complnt_hist,checkpoint_enctr_er_complnt_hist)