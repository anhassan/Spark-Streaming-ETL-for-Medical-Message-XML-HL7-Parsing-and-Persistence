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

def get_enctr_visit_rsn(file_path):
  
  row_tag = "ns0:HL7MessageEnvelope"
  num_cols = 8
  
  df = pdx.read_xml(file_path)
  message = pd.json_normalize(df[row_tag])
  
  msg_src_tag = "ns0:OriginatingRegion"
  csn_partial_tag = "ns0:PV1.PatientVisit.PV1.19.VisitNumber.ns0:CX.ExtendedCompositeIDWithCheckDigit.CX.1.IDNumber"
  msg_time_tag = "ns1:ReceivedDateTime"  
  
  try:
      csn_tag = get_fully_qualified_tag(message,csn_partial_tag)
      csn = message[csn_tag][0]
      msg_src = message[msg_src_tag][0]
      msg_time = message[msg_time_tag][0]
  except:
      return [num_cols*[None]]
  
  observation_partial_base_tag = "ns0:OBX.ObservationResult"
  identifier_tag = "OBX.3.ObservationIdentifier.ns0:OI.ObservationIdentifier.OI.1.Identifier"
  text_tag = "OBX.3.ObservationIdentifier.ns0:OI.ObservationIdentifier.OI.2.Text"
  visit_reason_tag = "OBX.5.ObservationValue_XAD.ns0:XAD.ExtendedAddress.XAD.9.CountyparishCode"
  visit_reason = []
  
  try:
      observation_base_tag = get_fully_qualified_obj_tag(message,observation_partial_base_tag)
      observations = message[observation_base_tag]
  except:
      observations = []
  for observation in observations:
    try:
      normalized_observation = pd.json_normalize(observation)
      if normalized_observation[identifier_tag][0] == "8661-1" and \
                               normalized_observation[text_tag][0] == "CHIEF COMPLAINT:FIND:PT:PATIENT:NOM:REPORTED:18100":
        reason = normalized_observation[visit_reason_tag][0]
        reason_filtered = "" if reason is None else reason
        visit_reason.append(reason_filtered)
    except:
        visit_reason.append("")
        
  msg_time = dateutil.parser.parse(msg_time)
  
  enctr_visit_rsn = [[msg_src,msg_time,int(csn),reason,None,None,None] for reason in visit_reason if len(reason) > 0]
  enctr_visit_rsn = [num_cols*[None]] if len(enctr_visit_rsn) == 0 else enctr_visit_rsn
  return enctr_visit_rsn

# COMMAND ----------

def get_encntr_visit_rsn_from_multiple_msgs(batch_msgs):
  mult_msgs = read_xml_batchs(batch_msgs)
  encntr_visit_rsn_lists = [ get_enctr_visit_rsn(msg) for msg in mult_msgs ]
  return [ item for sublist in encntr_visit_rsn_lists for item in sublist ]

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,ArrayType

insert_struct_fields = [StructField("row_updt_tsp",TimestampType(),True),
                        StructField("update_user_id",StringType(),True)]


enctr_visit_rsn_hist_struct_fields = [
                                      StructField("msg_src",StringType(),True),
                                      StructField("msg_tm",TimestampType(),True),
                                      StructField("pat_enc_csn_id",LongType(),True),
                                      StructField("encntr_rsn",StringType(),True),
                                      StructField("insert_user_id",StringType(),True)
                                      ]

enctr_visit_rsn_struct_fields = enctr_visit_rsn_hist_struct_fields + insert_struct_fields

enctr_visit_rsn_hist_schema = ArrayType(StructType(enctr_visit_rsn_hist_struct_fields))
enctr_visit_rsn_schema = ArrayType(StructType(enctr_visit_rsn_struct_fields))
enctr_visit_rsn_hist_schema_flat = StructType(enctr_visit_rsn_hist_struct_fields)                                              
enctr_visit_rsn_schema_flat = StructType(enctr_visit_rsn_struct_fields)

# COMMAND ----------

getEnctrVisitRsnUdf = udf(lambda x : get_enctr_visit_rsn(x),enctr_visit_rsn_schema)

# COMMAND ----------

getMultEnctrVisitRsnUdf = udf(lambda x : get_encntr_visit_rsn_from_multiple_msgs(x),enctr_visit_rsn_schema)

# COMMAND ----------

from pyspark.sql.functions import col, udf, explode

def invoke_enctr_visit_rsn_process(directory_path):
  return read_from_directory(directory_path)\
         .withColumn("parse",explode(getMultEnctrVisitRsnUdf(col('text'))))\
         .select("parse.*")

def write_enctr_visit_rsn_delta(df,output_table_name,checkpoint_loc):
   write_to_delta_sorted_with_logs(df.na.drop("all"),output_table_name,checkpoint_loc)

def write_enctr_visit_rsn_synapse(df,output_table_name,checkpoint_loc):
  write_stream_to_synapse(df.na.drop("all"),output_table_name,checkpoint_loc)
  
def insert_update_enctr_visit_rsn(df,insert_table,update_table,checkpoint_loc):
  insert_update_stream_to_delta(df.na.drop("all"),insert_table,update_table,checkpoint_loc,enctr_visit_rsn_hist_schema_flat,enctr_visit_rsn_schema_flat)