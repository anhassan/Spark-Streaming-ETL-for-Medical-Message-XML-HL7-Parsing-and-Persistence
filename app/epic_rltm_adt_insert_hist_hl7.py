# Databricks notebook source
# MAGIC %run ../utils/utilities

# COMMAND ----------

# MAGIC %run ../code/hl7/adt_hist

# COMMAND ----------

adt_hist = "epic_rltm.adt_hist_evh"
checkpoint_adt_hist = "/mnt/datalake/checkpoints_epic_rltm/hl7/hist/adt_hist"

# COMMAND ----------

# MAGIC %run ../code/hl7/encntr_dx_hist

# COMMAND ----------

encntr_dx_hist ="epic_rltm.encntr_dx_hist_evh"
checkpoint_encntr_dx_hist = "/mnt/datalake/checkpoints_epic_rltm/hl7/hist/encntr_dx_hist"

# COMMAND ----------

# MAGIC %run ../code/hl7/encntr_visit_rsn_hist

# COMMAND ----------

enctr_visit_rsn_hist ="epic_rltm.encntr_visit_rsn_hist_evh"
checkpoint_enctr_visit_rsn_hist = "/mnt/datalake/checkpoints_epic_rltm/hl7/hist/enctr_visit_rsn_hist"

# COMMAND ----------

# MAGIC %run ../code/hl7/encntr_er_complnt_hist

# COMMAND ----------

enctr_er_complnt_hist ="epic_rltm.encntr_er_complnt_hist_evh"
checkpoint_enctr_er_complnt_hist = "/mnt/datalake/checkpoints_epic_rltm/hl7/hist/enctr_er_complnt_hist"

# COMMAND ----------

# MAGIC %md
# MAGIC ###***Reading the Event Stream from Consumer Group***

# COMMAND ----------

event_hub_df = read_from_event_hub(eventhub_connection_string_consumer,eventhub_feed_name,eventhub_consumer_group)

# COMMAND ----------

# MAGIC %md
# MAGIC ###***Adt Hist Stream***

# COMMAND ----------

adt_dtl_df = invoke_adt_dtl_process(event_hub_df)
write_adt_dtl_delta(adt_dtl_df,adt_hist,checkpoint_adt_hist)

# COMMAND ----------

# MAGIC %md
# MAGIC ###***Encntr Dx Hist Stream***

# COMMAND ----------

encntr_dx_df = invoke_encntr_dx_process(event_hub_df)
write_encntr_dx_delta(encntr_dx_df,encntr_dx_hist,checkpoint_encntr_dx_hist)

# COMMAND ----------

# MAGIC %md
# MAGIC ###***Enctr Er Complaint Hist Stream***

# COMMAND ----------

encntr_er_complaint_df = invoke_encntr_er_complaint_process(event_hub_df)
write_encntr_er_complaint_delta(encntr_er_complaint_df,enctr_er_complnt_hist,checkpoint_enctr_er_complnt_hist)

# COMMAND ----------

# MAGIC %md
# MAGIC ###***Enctr Visit Reason Hist Stream***

# COMMAND ----------

encntr_visit_rsn_df = invoke_encntr_visit_rsn_process(event_hub_df)
write_encntr_visit_rsn_delta(encntr_visit_rsn_df,enctr_visit_rsn_hist,checkpoint_enctr_visit_rsn_hist)