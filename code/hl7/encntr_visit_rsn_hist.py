# Databricks notebook source
# MAGIC %run ../../utils/utilities

# COMMAND ----------

from datetime import datetime
from pytz import timezone
import dateutil.parser

# COMMAND ----------

def get_enctr_visit_rsn(message):
    #jsnmsg = parser.parse_message(message.replace('\n','\r'), find_groups = False, validation_level=2)
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
    
    
    observation_base_tag = "ns0:MessagePayload.ns0:AdministerPatient.VisitGroup.ns0:OBX.ObservationResult"
    identifier_tag = "OBX.3.ObservationIdentifier.ns0:OI.ObservationIdentifier.OI.1.Identifier"
    text_tag = "OBX.3.ObservationIdentifier.ns0:OI.ObservationIdentifier.OI.2.Text"
    visit_reason_tag = "OBX.5.ObservationValue_XAD.ns0:XAD.ExtendedAddress.XAD.9.CountyparishCode"
    visit_reason = []
    
    for obr in jsnmsg.OBX:
        
        try:
            identifier_tag = obr.OBX_3.CE_1.value if obr.OBX_3.CE_1.value else None 
        except:
            identifier_tag = None
        try:
            text_tag = obr.OBX_3.CE_2.value if obr.OBX_3.CE_2.value else None 
        except:
            text_tag = None 
            
        try:
            if identifier_tag == "8661-1" and text_tag == "CHIEF COMPLAINT:FIND:PT:PATIENT:NOM:REPORTED:18100":
                try:
                    reason = obr.OBX_5.VARIES_9.value if obr.OBX_5.VARIES_9.value else None 
                    reason_filtered = "" if reason is None else reason
                    visit_reason.append(reason_filtered)
                except:
                    visit_reason.append("")
        except:
              pass
    enctr_visit_rsn = [[msg_src,msg_time,int(csn),reason,None,None,None,datetime.now()] for reason in visit_reason if len(reason) > 0]
    enctr_visit_rsn = [num_cols*[None]] if len(enctr_visit_rsn) == 0 else enctr_visit_rsn
    return enctr_visit_rsn

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,ArrayType

insert_struct_fields = [StructField("row_updt_tsp",TimestampType(),True),
                        StructField("update_user_id",StringType(),True),
                       StructField("row_insert_tsp",TimestampType(),True)]


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

getEnctrVisitRsnUdf = udf(lambda x : get_enctr_visit_rsn(x),enctr_visit_rsn_hist_schema)

# COMMAND ----------

from pyspark.sql.functions import col, udf, explode

def invoke_encntr_visit_rsn_process(event_hub_df):
  return event_hub_df.withColumn("parse",explode(getEnctrVisitRsnUdf(col("text"))))\
                     .select("parse.*")


def write_encntr_visit_rsn_delta(df,output_table_name,checkpoint_loc):
  write_to_delta_sorted_with_logs_evh(df.na.drop("all"),output_table_name,checkpoint_loc)

# COMMAND ----------

