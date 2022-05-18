# Databricks notebook source
# MAGIC %run ../../utils/utilities

# COMMAND ----------

from datetime import datetime
from pytz import timezone
import dateutil.parser
from pyspark.sql.functions import col, udf, explode, pandas_udf

# COMMAND ----------

def get_encntr_dx(message):
    
    try:
        jsnmsg = parser.parse_message(message.replace('|2.3', '|2.5', 1).replace('\n','\r'), find_groups = False, validation_level=2)

    except:
        raise Exception("Invalid message")
    num_cols = 10

    try:
        msg_src = "STL"
        csn = int(jsnmsg.PV1.PV1_19.value) if jsnmsg.PV1.PV1_19.value else None 
        msgtime = jsnmsg.MSH.MSH_7.value if jsnmsg.MSH.MSH_7.value else None
#         msgtime = jsnmsg.MSH.MSH_7.value if jsnmsg.MSH.MSH_7.value else None
        msg_time = pytz.timezone('US/Central').localize(dateutil.parser.parse(msgtime))
    except:
        return [num_cols*[None]][0]
    
    dx_icd_cd,dx_nm,dx_code = [],[],[]
    encntr_dx = []
    
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
            if identifier_tag == "8661-1" and text_tag == "CHIEF COMPLAINT:FIND:PT:PATIENT:NOM:REPORTED":
                try:
                    dx_icd_cd_tag = obr.OBX_5.VARIES_1.value if obr.OBX_5.VARIES_1.value else None
                    dx_icd_cd.append(dx_icd_cd_tag)
                except:
                    dx_icd_cd.append(None)
                try:
                    dx_nm_tag = obr.OBX_5.VARIES_2.value if obr.OBX_5.VARIES_2.value else None
                    dx_nm.append(dx_nm_tag)
                except:
                    dx_nm.append(None)
                try:
                    dx_code_tag = obr.OBX_5.VARIES_3.value if obr.OBX_5.VARIES_3.value else None
                    dx_code.append(dx_code_tag)
                except:
                    dx_code.append(None)
        except:
              pass
    encntr_dx = [[msg_src,msg_time,int(csn),dx_icd_cd[ind],val,dx_code[ind],None,None,None,datetime.now()] for ind,val in enumerate(dx_nm) if val is not None or dx_code[ind] is not None or dx_icd_cd[ind] is not None]
    encntr_dx = [num_cols*[None]] if len(encntr_dx) == 0 else encntr_dx
    return encntr_dx

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,ArrayType

insert_struct_fields = [StructField("row_updt_tsp",TimestampType(),True),
                        StructField("update_user_id",StringType(),True),
                       StructField("row_insert_tsp",TimestampType(),True)]

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



def invoke_encntr_dx_process(event_hub_df):
  return event_hub_df.withColumn("parse",explode(getEncntrDxUdf(col('text'))))\
         .select("parse.*")

def write_encntr_dx_delta(df,output_table_name,checkpoint_loc):
   write_to_delta_sorted_with_logs_evh(df.na.drop("all"),output_table_name,checkpoint_loc)
