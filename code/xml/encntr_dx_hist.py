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

def get_encntr_dx(file_path):
  
  row_tag = "ns0:HL7MessageEnvelope"
  num_cols = 10
  
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
      return [num_cols*[None]]
  
  observation_partial_base_tag = "ns0:OBX.ObservationResult"
  identifier_tag = "OBX.3.ObservationIdentifier.ns0:OI.ObservationIdentifier.OI.1.Identifier"
  text_tag = "OBX.3.ObservationIdentifier.ns0:OI.ObservationIdentifier.OI.2.Text"
  dx_icd_cd_tag = "OBX.5.ObservationValue_XAD.ns0:XAD.ExtendedAddress.XAD.1.StreetAddress.ns0:SAD.StreetAddress.SAD.1.StreetOrMailingAddress"
  dx_nm_tag = "OBX.5.ObservationValue_XAD.ns0:XAD.ExtendedAddress.XAD.2.OtherDesignation"
  dx_code_tag = "OBX.5.ObservationValue_XAD.ns0:XAD.ExtendedAddress.XAD.3.City"
  dx_icd_cd,dx_nm,dx_code = [],[],[]
  encntr_dx = []
  
  try:
      observation_base_tag = get_fully_qualified_obj_tag(message,observation_partial_base_tag)
      observations = message[observation_base_tag]
  except:
      observations = []
  for observation in observations:
    try:
      normalized_observation = pd.json_normalize(observation)
      if normalized_observation[identifier_tag][0] == "8661-1" and \
              normalized_observation[text_tag][0] == "CHIEF COMPLAINT:FIND:PT:PATIENT:NOM:REPORTED":
        try:
          dx_icd_cd.append(normalized_observation[dx_icd_cd_tag][0])
        except:
          dx_icd_cd.append(None)
        try:
          dx_nm.append(normalized_observation[dx_nm_tag][0])
        except:
          dx_nm.append(None)
        try:
          dx_code.append(normalized_observation[dx_code_tag][0])
        except:
          dx_code.append(None)
    except:
      pass
  
  msg_time = dateutil.parser.parse(msg_time)
  encntr_dx = [[msg_src,msg_time,int(csn),dx_icd_cd[ind],val,dx_code[ind],None,None,None] for ind,val in enumerate(dx_nm) \
              if val is not None or dx_code[ind] is not None or dx_icd_cd[ind] is not None]
  encntr_dx = [num_cols*[None]] if len(encntr_dx) == 0 else encntr_dx
  return encntr_dx
      

# COMMAND ----------

def get_encntr_dx_from_multiple_msgs(batch_msgs):
  mult_msgs = read_xml_batchs(batch_msgs)
  encntr_dx_lists = [ get_encntr_dx(msg) for msg in mult_msgs ]
  return [ item for sublist in encntr_dx_lists for item in sublist]

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,ArrayType

insert_struct_fields = [StructField("row_updt_tsp",TimestampType(),True),
                        StructField("update_user_id",StringType(),True)]

encntr_dx_hist_struct_fields = [
                          StructField("msg_src",StringType(),True),
                          StructField("msg_tm",TimestampType(),True),
                          StructField("pat_enc_csn_id",LongType(),True),
                          StructField("dx_icd_cd",StringType(),True),
                          StructField("dx_name",StringType(),True),
                          StructField("dx_code_type",StringType(),True),
                          StructField("insert_user_id",StringType(),True),
                          ]

encntr_dx_struct_fields = encntr_dx_hist_struct_fields + insert_struct_fields

encntr_dx_hist_schema = ArrayType(StructType(encntr_dx_hist_struct_fields))
encntr_dx_schema = ArrayType(StructType(encntr_dx_struct_fields))

encntr_dx_hist_schema_flat = StructType(encntr_dx_hist_struct_fields)
encntr_dx_schema_flat = StructType(encntr_dx_struct_fields)

# COMMAND ----------

getEncntrDxUdf = udf(lambda x : get_encntr_dx(x),encntr_dx_schema)

# COMMAND ----------

getMultEncntrDxUdf = udf(lambda x : get_encntr_dx_from_multiple_msgs(x),encntr_dx_schema)

# COMMAND ----------

from pyspark.sql.functions import col, udf, explode

def invoke_encntr_dx_process(directory_path):
  return read_from_directory(directory_path)\
         .withColumn("parse",explode(getMultEncntrDxUdf(col('text'))))\
         .select("parse.*")

def write_encntr_dx_delta(df,output_table_name,checkpoint_loc):
   write_to_delta_sorted_with_logs(df.na.drop("all"),output_table_name,checkpoint_loc)

def write_encntr_dx_synapse(df,output_table_name,checkpoint_loc):
  write_stream_to_synapse(df.na.drop("all"),output_table_name,checkpoint_loc)

def insert_update_encntr_dx(df,insert_table,update_table,checkpoint_loc):
  insert_update_stream_to_delta(df.na.drop("all"),insert_table,update_table,checkpoint_loc,encntr_dx_hist_schema_flat,encntr_dx_schema_flat)