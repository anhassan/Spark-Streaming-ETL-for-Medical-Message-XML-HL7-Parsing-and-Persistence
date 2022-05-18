# Databricks notebook source
# MAGIC %run ../../utils/utilities

# COMMAND ----------

from datetime import datetime
from pytz import timezone
import dateutil.parser

# COMMAND ----------

def get_enctr_er_complnt(message):
    try:
        jsnmsg = parser.parse_message(message.replace('|2.3', '|2.5', 1).replace('\n','\r'), find_groups = False, validation_level=2)
    except:
        raise Exception("Invalid message")
    num_cols = 8
    
    try:
        msg_src = "STL"
        csn = int(jsnmsg.PV1.PV1_19.value) if jsnmsg.PV1.PV1_19.value else None
        msgtime = jsnmsg.MSH.MSH_7.value if jsnmsg.MSH.MSH_7.value else None
        msg_time = pytz.timezone('US/Central').localize(dateutil.parser.parse(msgtime))
    except:
        return [num_cols*[None]]
    
    diagnosis_type_tag = "ns0:MessagePayload.ns0:AdministerPatient.ns0:DG1.Diagnosis.DG1.6.DiagnosisType"
    diagnosis_description_tag ="ns0:MessagePayload.ns0:AdministerPatient.ns0:DG1.Diagnosis.DG1.4.DiagnosisDescription"
    
    try:
        diagnosis_type_tag = jsnmsg.DG1.DG1_6.value if jsnmsg.DG1.DG1_6.value else None
    except:
        diagnosis_type_tag = None
        
    try:
        diagnosis_description_tag = jsnmsg.DG1.DG1_4.value if jsnmsg.DG1.DG1_4.value else None
    except:
        diagnosis_description_tag = None
    
    try:
        er_complaint = diagnosis_description_tag if diagnosis_type_tag == "10800;EPT" else [] 
        
        enctr_er_complnt = [msg_src,msg_time,int(csn),er_complaint,None,None,None,datetime.now()] if len(er_complaint) > 0 else []
    except:
        enctr_er_complnt = []
  
    enctr_er_complnt = num_cols*[None] if len(enctr_er_complnt) == 0 else enctr_er_complnt
    return enctr_er_complnt

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,ArrayType

insert_struct_fields = [StructField("row_updt_tsp",TimestampType(),True),
                        StructField("update_user_id",StringType(),True),
                        StructField("row_insert_tsp",TimestampType(),True)]

enctr_er_complnt_hist_struct_fields = [
                                      StructField("msg_src",StringType(),True),
                                      StructField("msg_tm",TimestampType(),True),
                                      StructField("pat_enc_csn_id",LongType(),True),
                                      StructField("er_complaint",StringType(),True),
                                      StructField("insert_user_id",StringType(),True)
                                      ]

enctr_er_complnt_struct_fields = enctr_er_complnt_hist_struct_fields + insert_struct_fields

enctr_er_complnt_hist_schema = StructType(enctr_er_complnt_hist_struct_fields)
enctr_er_complnt_schema = StructType(enctr_er_complnt_struct_fields)

# COMMAND ----------

getEnctrErComplntUdf = udf(lambda x : get_enctr_er_complnt(x),enctr_er_complnt_hist_schema)

# COMMAND ----------

from pyspark.sql.functions import col, udf, explode

def invoke_encntr_er_complaint_process(event_hub_df):
  return event_hub_df.withColumn("parse",getEnctrErComplntUdf(col("text")))\
                     .select("parse.*")

def write_encntr_er_complaint_delta(df,output_table_name,checkpoint_loc):
   write_to_delta_sorted_with_logs_evh(df.na.drop("all"),output_table_name,checkpoint_loc)



# COMMAND ----------

