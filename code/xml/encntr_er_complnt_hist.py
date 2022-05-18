# Databricks notebook source
# MAGIC %run ../../utils/utilities

# COMMAND ----------

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", access_key)

# COMMAND ----------

import pandas as pd
import pandas_read_xml as pdx
from datetime import datetime
from pytz import timezone
import dateutil.parser

def get_enctr_er_complnt(file_path):
  
  row_tag = "ns0:HL7MessageEnvelope"
  num_cols = 8
  
  df = pdx.read_xml(file_path)
  doc = df[row_tag]
  message = pd.json_normalize(doc)
  
  msg_src_tag = "ns0:OriginatingRegion"
  csn_partial_tag = "ns0:PV1.PatientVisit.PV1.19.VisitNumber.ns0:CX.ExtendedCompositeIDWithCheckDigit.CX.1.IDNumber"
  msg_time_tag = "ns1:ReceivedDateTime"
  try:
      csn_tag = get_fully_qualified_tag(message,csn_partial_tag)
      csn = message[csn_tag][0]
      msg_src = message[msg_src_tag][0]
      msg_time = message[msg_time_tag][0]
  except:
      return num_cols*[None]
  
  diagnosis_type_partial_tag = "ns0:DG1.Diagnosis.DG1.6.DiagnosisType"
  diagnosis_description_partial_tag ="ns0:DG1.Diagnosis.DG1.4.DiagnosisDescription"
  try:
      diagnosis_type_tag = get_fully_qualified_tag(message,diagnosis_type_partial_tag)
      diagnosis_description_tag = get_fully_qualified_tag(message,diagnosis_description_partial_tag)
      er_complaint = message[diagnosis_description_tag][0] if message[diagnosis_type_tag][0] == "10800;EPT" else []
      msg_time = dateutil.parser.parse(msg_time)
      enctr_er_complnt = [msg_src,msg_time,int(csn),er_complaint,None,None,None] if len(er_complaint) > 0 else []
  except:
      enctr_er_complnt = []
  
  enctr_er_complnt = num_cols*[None] if len(enctr_er_complnt) == 0 else enctr_er_complnt
  return enctr_er_complnt


# COMMAND ----------

def get_encntr_er_complnt_from_multiple_msgs(batch_msgs):
  mult_msgs = read_xml_batchs(batch_msgs)
  encntr_er_complnt_lists = [ get_enctr_er_complnt(msg) for msg in mult_msgs ]
  return [ item for sublist in encntr_er_complnt_lists for item in sublist ]

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,ArrayType

insert_struct_fields = [StructField("row_updt_tsp",TimestampType(),True),
                        StructField("update_user_id",StringType(),True)]

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

getEnctrErComplntUdf = udf(lambda x : get_enctr_er_complnt(x),enctr_er_complnt_schema)

# COMMAND ----------

getMultEncntrErComplntUdf = udf(lambda x : get_encntr_er_complnt_from_multiple_msgs(x),enctr_er_complnt_schema)

# COMMAND ----------

def invoke_enctr_er_complnt_process(directory_path):
  return read_from_directory(directory_path)\
         .withColumn("parse",getMultEncntrErComplntUdf(col('text')))\
         .select("parse.*")

def write_enctr_er_complnt_delta(df,output_table_name,checkpoint_loc):
   write_to_delta_sorted_with_logs(df.na.drop("all"),output_table_name,checkpoint_loc)

def write_enctr_er_complnt_synapse(df,output_table_name,checkpoint_loc):
  write_stream_to_synapse(df.na.drop("all"),output_table_name,checkpoint_loc)

def insert_update_enctr_er_complnt(df,insert_table,update_table,checkpoint_loc):
  insert_update_stream_to_delta(df.na.drop("all"),insert_table,update_table,checkpoint_loc,enctr_er_complnt_hist_schema,enctr_er_complnt_schema)