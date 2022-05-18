# Databricks notebook source
# MAGIC %run ../utils/utilities

# COMMAND ----------

adt_hist_delta = "epic_rltm.adt_hist"
adt_hist_synapse = "adt_hist"
checkpoint_adt_hist_synapse = "/mnt/datalake/checkpoints_epic_rltm/synapse/adt_dtl_hist"

# COMMAND ----------

encntr_dx_hist_delta ="epic_rltm.encntr_dx_hist"
encntr_dx_hist_synapse ="encntr_dx_hist"
checkpoint_encntr_dx_hist_synapse = "/mnt/datalake/checkpoints_epic_rltm/synapse/encntr_dx_hist"

# COMMAND ----------

enctr_visit_rsn_hist_delta ="epic_rltm.encntr_visit_rsn_hist"
enctr_visit_rsn_hist_synapse ="encntr_visit_rsn_hist_synapse"
checkpoint_enctr_visit_rsn_hist_synapse = "/mnt/datalake/checkpoints_epic_rltm/synapse/enctr_visit_rsn_hist"

# COMMAND ----------

enctr_er_complnt_hist_delta ="epic_rltm.encntr_er_complnt_hist"
enctr_er_complnt_hist_synapse ="encntr_er_complnt_hist"
checkpoint_enctr_er_complnt_hist_synapse = "/mnt/datalake/checkpoints_epic_rltm/synapse/enctr_er_complnt_hist"

# COMMAND ----------

copy_to_synapse(adt_hist_delta,adt_hist_synapse,checkpoint_adt_hist_synapse)

# COMMAND ----------

copy_to_synapse(encntr_dx_hist_delta,encntr_dx_hist_synapse,checkpoint_encntr_dx_hist_synapse)

# COMMAND ----------

copy_to_synapse(enctr_visit_rsn_hist_delta,enctr_visit_rsn_hist_synapse,checkpoint_enctr_visit_rsn_hist_synapse)

# COMMAND ----------

copy_to_synapse(enctr_er_complnt_hist_delta,enctr_er_complnt_hist_synapse,checkpoint_enctr_er_complnt_hist_synapse)