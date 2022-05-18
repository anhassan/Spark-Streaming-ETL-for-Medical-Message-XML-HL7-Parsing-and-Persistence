# Databricks notebook source
# MAGIC %run ../../utils/utilities

# COMMAND ----------

import dateutil.parser
from dateutil.tz import gettz
import pytz
from datetime import datetime
import dateutil.parser
import json
import hl7
from hl7apy import parser
from hl7apy.core import Group, Segment
from pyspark.sql.functions import *

# COMMAND ----------

# spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", access_key)

# COMMAND ----------

def get_acuity_level(message):
    delimiter = "|||"
    acuity_level = ""
    for obr in message.OBX:
        
        try:
            identifier_tag = obr.OBX_3.CE_1.value if obr.OBX_3.CE_1.value else None
        except:
            identifier_tag = None
        try:
            text_tag = obr.OBX_3.CE_2.value if obr.OBX_3.CE_2.value else None
        except:
            text_tag = None
        try:
            acuity_tag = obr.OBX_5.VARIES_2.value if obr.OBX_5.VARIES_2.value else None
        except:
            acuity_tag = None
                
        try:
            if identifier_tag == "11283-9" and text_tag == "ACUITY ASSESSMENT AT FIRST ENCOUNTER":
                acuity_value = acuity_tag
            
            if len(acuity_level) == 0 :
                acuity_level += acuity_value
            else:
                acuity_level = acuity_level + delimiter + acuity_value
        except:
            pass
    
    acuity_level_filtered = None if len(acuity_level) == 0 else acuity_level
    return acuity_level_filtered

# COMMAND ----------

def get_adt_arrival_time(message):
    try:
        trigger_type = message.MSH.MSH_9.MSG_2.value if message.MSH.MSH_9.MSG_2.value else None
    except:
        trigger_type = None 

    try:
        adt_arrival_time = message.PV1.PV1_44.value if message.PV1.PV1_44.value else None
    except:
        adt_arrival_time = None

      
    if trigger_type == "A04":
        try:
            adt_arrival_time_filtered = pytz.timezone('US/Central').localize(dateutil.parser.parse(adt_arrival_time)) if adt_arrival_time else None
            return adt_arrival_time_filtered
        except:
            return None
    return None

# COMMAND ----------

def get_hosp_admsn_time(message):
    try:
        trigger_type = message.MSH.MSH_9.MSG_2.value if message.MSH.MSH_9.MSG_2.value else None
    except:
        trigger_type = None
    
    try:
        hosp_admsn_time = message.PV1.PV1_44.value if message.PV1.PV1_44.value else None
    except:
        hosp_admsn_time = None
    
    if trigger_type == "A01" or trigger_type == "A11":
        try:
            hosp_admsn_time_filtered = hosp_admsn_time if hosp_admsn_time is None else pytz.timezone('US/Central').localize(dateutil.parser.parse(hosp_admsn_time))
            return hosp_admsn_time_filtered
        except:
            return None
      
    return None

# COMMAND ----------

def get_hosp_disch_time(message):
    try:
        trigger_type = message.MSH.MSH_9.MSG_2.value if message.MSH.MSH_9.MSG_2.value else None
    except:
        trigger_type = None
        
    try:
        hosp_disch_time = message.PV1.PV1_45.value if message.PV1.PV1_45.value else None
    except:
        hosp_disch_time = None
    
    if trigger_type == "A03" or trigger_type == "A13":
        try:
            hosp_disch_time_filtered = hosp_disch_time if hosp_disch_time is None else pytz.timezone('US/Central').localize(dateutil.parser.parse(hosp_disch_time))
            return hosp_disch_time_filtered
        except:
            return None
      
    return None

# COMMAND ----------

def get_adt_dtl(message):
    #jsnmsg = parser.parse_message(message.replace('\n','\r'), find_groups = False, validation_level=2)
    try:
        jsnmsg = parser.parse_message(message.replace('|2.3', '|2.5', 1).replace('\n','\r'), find_groups = False, validation_level=2)
    except:
        raise Exception("Invalid message")      
     
    try:
        MessageCode = jsnmsg.MSH.MSH_9.MSG_1.value if jsnmsg.MSH.MSH_9.MSG_1.value else None
    except:
        MessageCode = None
    
    OriginatingRegion = "STL"
    
    try:
        TriggerEvent = jsnmsg.MSH.MSH_9.MSG_2.value if jsnmsg.MSH.MSH_9.MSG_2.value else None
    except:
        TriggerEvent = None
        
    try:
        BODID = jsnmsg.MSH.MSH_10.value if jsnmsg.MSH.MSH_10.value else None
    except:
        BODID = None
    
    try:
        dob = jsnmsg.PID.PID_7.value if jsnmsg.PID.PID_7.value else None 
        DatetimeOfBirth = pytz.timezone('US/Central').localize(dateutil.parser.parse(dob))
    except:
        DatetimeOfBirth = None
        
    try:
        dod = jsnmsg.PID.PID_29.value if jsnmsg.PID.PID_29.value else None
        PatientDeathDateAndTime = pytz.timezone('US/Central').localize(dateutil.parser.parse(dod))
    except:
        PatientDeathDateAndTime = None
        
    try:
        PatientClass = jsnmsg.PV1.PV1_2.value if jsnmsg.PV1.PV1_2.value else None  
    except:
        PatientClass = None
    
    try:
        PointOfCare = jsnmsg.PV1.PV1_3.PL_1.value if jsnmsg.PV1.PV1_3.PL_1.value else None 
    except:
        PointOfCare = None
    
    try:
        HierarchicDesignator = jsnmsg.EVN.EVN_5.XCN_14.value  if jsnmsg.EVN.EVN_5.XCN_14.value else None
    except:
        HierarchicDesignator = None
        
    try:
        Room = jsnmsg.PV1.PV1_3.PL_2.value if jsnmsg.PV1.PV1_3.PL_2.value else None 
    except:
        Room = None
        
    try:
        Bed = jsnmsg.PV1.PV1_3.PL_3.value if jsnmsg.PV1.PV1_3.PL_3.value else None
    except:
        Bed = None
        
    try:
        LocationStatus = jsnmsg.PV1.PV1_3.PL_5.value if jsnmsg.PV1.PV1_3.PL_5.value else None 
    except:
        LocationStatus = None
        
    try:
        sex = jsnmsg.PID.PID_8.value if jsnmsg.PID.PID_8.value else None
    except:
        sex = None
        
    try:
        #### need valid data to validate 
        ModeOfArrivalCode = jsnmsg.PV2.PV2_38.value if jsnmsg.PV2.PV2_38.value else None
    except:
        ModeOfArrivalCode = None
        
    try:
        #### need valid data to validate 
        EDDisposition = jsnmsg.ZPV.ZPV_15.value if jsnmsg.ZPV.ZPV_15.value else None 
    except:
        EDDisposition = None
        
    try:
        #### need valid data to validate 
        DischargeDisposition = jsnmsg.PV1.PV1_36.value if jsnmsg.PV1.PV1_36.value else None
    except:
        DischargeDisposition = None
        
    try:
        AccommodationCode = jsnmsg.PV2.PV2_2.value if jsnmsg.PV2.PV2_2.value else None
    except:
        AccommodationCode = None
    
    try:
        OperatorID = jsnmsg.EVN.EVN_5.XCN_1.value if jsnmsg.EVN.EVN_5.XCN_1.value else None 
    except:
        OperatorID = None
        
    adt_dtl = [
        MessageCode,
        OriginatingRegion,
        TriggerEvent,
        BODID,
        DatetimeOfBirth,
        PatientDeathDateAndTime,
        PatientClass,
        PointOfCare,
        HierarchicDesignator,
        Room,
        Bed,
        LocationStatus,
        sex,
        ModeOfArrivalCode,
        EDDisposition,
        DischargeDisposition,
        AccommodationCode,
        OperatorID
       ]
    
    try:
        msgtime = jsnmsg.MSH.MSH_7.value if jsnmsg.MSH.MSH_7.value else None 
        msg_time = pytz.timezone('US/Central').localize(dateutil.parser.parse(msgtime))
        adt_dtl.append(msg_time)
    except:
        adt_dtl.append(None)
        
      
    try:
        hsp_account_id = int(jsnmsg.PV1.PV1_50.value) if jsnmsg.PV1.PV1_50.value else None 
        adt_dtl.append(hsp_account_id)
    except:
        adt_dtl.append(None)
        
    try:
        csn_id = int(jsnmsg.PV1.PV1_19.value) if jsnmsg.PV1.PV1_19.value else None 
        adt_dtl.append(csn_id)
    except:
        adt_dtl.append(None)
    
    try:
        pat_mrn_id = jsnmsg.PID.PID_3.CX_1.value if jsnmsg.PID.PID_3.CX_1.value else None
    except:
        pat_mrn_id = None
    
    adt_dtl = adt_dtl + [get_acuity_level(jsnmsg),get_adt_arrival_time(jsnmsg),pat_mrn_id,\
                         get_hosp_admsn_time(jsnmsg),get_hosp_disch_time(jsnmsg),datetime.now(),None,None,None]
    
    return adt_dtl


# COMMAND ----------

# MAGIC %sql
# MAGIC select current_user

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,ArrayType,DecimalType

insert_struct_fields = [StructField("row_updt_tsp",TimestampType(),True),
                      StructField("update_user_id",StringType(),True)]

adt_dtl_hist_struct_fields = [StructField("msg_typ",StringType(),True),
                        StructField("msg_src",StringType(),True),
                        StructField("trigger_evnt",StringType(),True),
                        StructField("bod_id",StringType(),True),
                        StructField("birth_date",TimestampType(),True),
                        StructField("death_date",TimestampType(),True),
                        StructField("pat_class_abbr",StringType(),True),
                        StructField("department_abbr",StringType(),True),
                        StructField("loc_abbr",StringType(),True),
                        StructField("room_nm",StringType(),True),
                        StructField("bed_label",StringType(),True),
                        StructField("bed_status",StringType(),True),
                        StructField("sex_abbr",StringType(),True),
                        StructField("means_of_arrv_abbr",StringType(),True),
                        StructField("ed_disposition_abbr",StringType(),True),
                        StructField("disch_disp_abbr",StringType(),True),
                        StructField("accommodation_abbr",StringType(),True),
                        StructField("user_id",StringType(),True),
                        StructField("msg_tm",TimestampType(),True),
                        StructField("hsp_account_id",LongType(),True), #Long
                        StructField("pat_enc_csn_id",LongType(),True), #Long
                        StructField("acuity_level_abbr",StringType(),True),
                        StructField("adt_arrival_time",TimestampType(),True),
                        StructField("pat_mrn_id",StringType(),True),
                        StructField("hosp_admsn_time",TimestampType(),True),
                        StructField("hosp_disch_time",TimestampType(),True),
                        StructField("row_insert_tsp",TimestampType(),True),
                        StructField("insert_user_id",StringType(),True)]

adt_dtl_struct_fields = adt_dtl_hist_struct_fields + insert_struct_fields

adt_dtl_schema = StructType(adt_dtl_struct_fields)
adt_dtl_hist_schema = StructType(adt_dtl_hist_struct_fields)

# COMMAND ----------

getAdtDtlUdf = udf(lambda x : get_adt_dtl(x),adt_dtl_hist_schema)

# COMMAND ----------

from pyspark.sql.functions import *

def invoke_adt_dtl_process(event_hub_df):
  return event_hub_df.withColumn("parse",getAdtDtlUdf(col("text")))\
                     .select(*["parse.*","msg_nm"])

def write_adt_dtl_delta(df,output_table_name,checkpoint_loc):
  adt_cols = df.columns
  adt_cols.remove("msg_nm")
  df = df.na.drop(how="all", subset=adt_cols)
  write_to_delta_sorted_with_logs_evh(df,output_table_name,checkpoint_loc)
         

# COMMAND ----------

