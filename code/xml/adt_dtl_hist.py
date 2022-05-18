# Databricks notebook source
# MAGIC %run ../../utils/utilities

# COMMAND ----------

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", access_key)

# COMMAND ----------

row_tag = "ns0:HL7MessageEnvelope"
  
tags = [
        "MSG.1.MessageCode",
        "OriginatingRegion",
        "MSG.2.TriggerEvent",
        "ns1:BODID",
        "PID.7.DatetimeOfBirth.ns0:TS.TimeStamp.TS.1.Time",
        "PID.29.PatientDeathDateAndTime.ns0:TS.TimeStamp.TS.1.Time",
        "PV1.2.PatientClass",
        "PV1.3.AssignedPatientLocation.ns0:PL.PersonLocation.PL.1.PointOfCare",
        "EVN.5.OperatorID.ns0:XCN.ExtendedCompositeIDNumberAndNameForPersons.XCN.14.AssigningFacility.ns0:HD.HierarchicDesignator.HD.1.NamespaceID",
        "PV1.3.AssignedPatientLocation.ns0:PL.PersonLocation.PL.2.Room",
        "PV1.3.AssignedPatientLocation.ns0:PL.PersonLocation.PL.3.Bed.ns0:HD.HierarchicDesignator.HD.1.NamespaceID",
        "PV1.3.AssignedPatientLocation.ns0:PL.PersonLocation.PL.5.LocationStatus",
        "PID.8.Sex",
        "PV2.38.ModeOfArrivalCode.ns0:CE.CodedElement.CE.1.Identifier",
        "ZPV.15.EDDisposition",
        "PV1.36.DischargeDisposition",
        "ns0:PV2.AdditionalInformation.PV2.2.AccommodationCode.ns0:CE.CodedElement.CE.1.Identifier",
        "ns0:EVN.EventType.EVN.5.OperatorID.ns0:XCN.ExtendedCompositeIDNumberAndNameForPersons.XCN.1.IDNumber"
       ]


# COMMAND ----------

def get_pat_mrn_id(message):
  
  patient_id_partial_tag = "ns0:PID.PatientIdentification.PID.3.PatientIdentifierList"
  mrn_base_tag = "ns0:CX.ExtendedCompositeIDWithCheckDigit"
  mrn_id_tag = "CX.1.IDNumber"
  pat_mrn_ids = []
  
  try:
    patient_id_tag = get_fully_qualified_obj_tag(message,patient_id_partial_tag)
    patient_ids = message[patient_id_tag][0]
  except:
    try:
        pat_mrn_id = message["{}.{}.{}".format(patient_id_tag,mrn_base_tag,mrn_id_tag)][0]
        return pat_mrn_id
    except:
        return None
  
  for patient_id in patient_ids:
    try:
      pat_mrn_id = patient_id[mrn_base_tag][mrn_id_tag]
      pat_mrn_ids.append(pat_mrn_id)
    except:
      pat_mrn_ids.append(None)
  
  return pat_mrn_ids[0]

# COMMAND ----------

def get_acuity_level(message):
  
    observation_partial_base_tag = "ns0:OBX.ObservationResult"
    identifier_tag = "OBX.3.ObservationIdentifier.ns0:OI.ObservationIdentifier.OI.1.Identifier"
    text_tag = "OBX.3.ObservationIdentifier.ns0:OI.ObservationIdentifier.OI.2.Text"
    acuity_tag = "OBX.5.ObservationValue_XAD.ns0:XAD.ExtendedAddress.XAD.2.OtherDesignation"
    
    try:
        observation_base_tag = get_fully_qualified_obj_tag(message,observation_partial_base_tag)
        observations = list(message[observation_base_tag])[0]
        
    except:
       return None
      
    delimiter = "|||"
    acuity_level = ""
    
    for observation in observations:
      try:
        normalized_observation = pd.json_normalize(observation)
        if normalized_observation[identifier_tag][0] == "11283-9" and \
                                 normalized_observation[text_tag][0] == "ACUITY ASSESSMENT AT FIRST ENCOUNTER":
          acuity_value = normalized_observation[acuity_tag][0]
          if len(acuity_level) == 0 :
             acuity_level += acuity_value
          else:
             acuity_level = acuity_level + delimiter + acuity_value
      except:
          pass
        
    acuity_level_filtered = None if len(acuity_level) == 0 else acuity_level
    return acuity_level_filtered 

# COMMAND ----------

import dateutil.parser
import pytz
def get_adt_arrival_time(message):
  
  trigger_type_partial_tag = "ns0:MSH.MessageHeader.MSH.9.MessageType.ns0:MSG.MessageType.MSG.2.TriggerEvent"
  adt_arrival_partial_tag = "ns0:PV1.PatientVisit.PV1.44.AdmitDatetime.ns0:TS.TimeStamp.TS.1.Time"
  
  try:
      trigger_type_tag = get_fully_qualified_tag(message,trigger_type_partial_tag)
      trigger_type = message[trigger_type_tag][0]
  except:
      return None
      
  if trigger_type == "A04":
      try:
          adt_arrival_tag = get_fully_qualified_tag(message,adt_arrival_partial_tag)
          adt_arrival_time = message[adt_arrival_tag][0]
          adt_arrival_time_filtered = adt_arrival_time if adt_arrival_time is None else pytz.timezone('US/Central').localize(dateutil.parser.parse(adt_arrival_time))
          return adt_arrival_time_filtered
      except:
          return None
  return None

# COMMAND ----------

import pytz
import dateutil.parser

def get_hosp_admsn_time(message):
  
  trigger_type_partial_tag = "ns0:MSH.MessageHeader.MSH.9.MessageType.ns0:MSG.MessageType.MSG.2.TriggerEvent"
  hosp_admsn_time_partial_tag = "ns0:PV1.PatientVisit.PV1.44.AdmitDatetime.ns0:TS.TimeStamp.TS.1.Time"
  
  try:
      trigger_type_tag = get_fully_qualified_tag(message,trigger_type_partial_tag)
      trigger_type = message[trigger_type_tag][0]
  except:
      return None
    
  if trigger_type == "A01" or trigger_type == "A11":
    try:
        hosp_admsn_time_tag = get_fully_qualified_tag(message,hosp_admsn_time_partial_tag)
        hosp_admsn_time = message[hosp_admsn_time_tag][0]
        hosp_admsn_time_filtered = hosp_admsn_time if hosp_admsn_time is None else pytz.timezone('US/Central').localize(dateutil.parser.parse(hosp_admsn_time))
        return hosp_admsn_time_filtered
    except:
        return None
      
  return None
  

# COMMAND ----------

import pytz
import dateutil.parser

def get_hosp_disch_time(message):
  
  trigger_type_partial_tag = "ns0:MSH.MessageHeader.MSH.9.MessageType.ns0:MSG.MessageType.MSG.2.TriggerEvent"
  hosp_disch_time_partial_tag = "ns0:PV1.PatientVisit.PV1.45.DischargeDatetime.ns0:TS.TimeStamp.TS.1.Time"
  
  try:
      trigger_type_tag = get_fully_qualified_tag(message,trigger_type_partial_tag)
      trigger_type = message[trigger_type_tag][0]
  except:
      return None
    
  if trigger_type == "A03" or trigger_type == "A13":
    try:
        hosp_disch_time_tag = get_fully_qualified_tag(message,hosp_disch_time_partial_tag)
        hosp_disch_time = message[hosp_disch_time_tag][0]
        hosp_disch_time_filtered = hosp_disch_time if hosp_disch_time is None else pytz.timezone('US/Central').localize(dateutil.parser.parse(hosp_disch_time))
        return hosp_disch_time_filtered
    except:
        return None
      
  return None

# COMMAND ----------

import pandas as pd
import pandas_read_xml as pdx
import dateutil.parser
from dateutil.tz import gettz
import pytz
from datetime import datetime
import dateutil.parser

def get_adt_dtl(file_path,row_tag,tags):
  
  df = pdx.read_xml(file_path)
  doc = df[row_tag]
  doc_normalized = pd.json_normalize(doc)
  message_cols = list(doc_normalized.columns)
  
  time_tag = "ns0:TS.TimeStamp.TS.1.Time"
  
  msg_src_tag = "ns0:OriginatingRegion"
  msg_time_tag = "ns1:ReceivedDateTime"
  csn_partial_tag = "ns0:PV1.PatientVisit.PV1.19.VisitNumber.ns0:CX.ExtendedCompositeIDWithCheckDigit.CX.1.IDNumber"
  hsp_account_id_partial_tag = "ns0:PV1.PatientVisit.PV1.50.AlternateVisitID.ns0:CX.ExtendedCompositeIDWithCheckDigit.CX.1.IDNumber"
  adt_dtl = []
  
  for tag in tags:
    tag_fqdn = [col for col in message_cols if col.endswith(tag)]
    
    if len(tag_fqdn) > 0:
        adt_dtl_val = doc_normalized[tag_fqdn[0]][0]
        
        if tag_fqdn[0].endswith(time_tag):
          
            adt_dtl_val_filtered = adt_dtl_val if adt_dtl_val is None else \
                                   pytz.timezone('US/Central').localize(dateutil.parser.parse(adt_dtl_val))
            
        else:
            adt_dtl_val_filtered = adt_dtl_val
        adt_dtl.append(adt_dtl_val_filtered)
    else:
        adt_dtl.append(None)
        
  try:
    msg_time = doc_normalized[msg_time_tag][0]
    adt_dtl.append(dateutil.parser.parse(msg_time))
  except:
    adt_dtl.append(None)
    
  try:
    hsp_account_id_tag = get_fully_qualified_tag(doc_normalized,hsp_account_id_partial_tag)
    hsp_account_id = int(doc_normalized[hsp_account_id_tag][0])
    adt_dtl.append(hsp_account_id)
  except:
    adt_dtl.append(None)
    
  try:
    csn_tag = get_fully_qualified_tag(doc_normalized,csn_partial_tag)
    csn_id = int(doc_normalized[csn_tag][0])
    adt_dtl.append(csn_id)
  except:
    adt_dtl.append(None)
  
  adt_dtl = adt_dtl + [get_acuity_level(doc_normalized),get_adt_arrival_time(doc_normalized), \
                       get_pat_mrn_id(doc_normalized),get_hosp_admsn_time(doc_normalized),get_hosp_disch_time(doc_normalized),None,None,None]
  
  return adt_dtl

# COMMAND ----------

def get_adt_from_multiple_msgs(batch_msgs,row_tag,tags):
  mult_msgs = read_xml_batchs(batch_msgs)
  return [ get_adt_dtl(msg,row_tag,tags) for msg in mult_msgs]
  

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,ArrayType

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
                        StructField("insert_user_id",StringType(),True)]

adt_dtl_struct_fields = adt_dtl_hist_struct_fields + insert_struct_fields

adt_dtl_schema = ArrayType(StructType(adt_dtl_struct_fields))
adt_dtl_hist_schema = ArrayType(StructType(adt_dtl_hist_struct_fields))

# COMMAND ----------

def getAdtDtlUdf(row_tag,tags):
  return udf(lambda x : get_adt_dtl(x,row_tag,tags),adt_dtl_schema)

# COMMAND ----------

def getMultAdtDtlUdf(row_tag,tags):
  return udf(lambda x : get_adt_from_multiple_msgs(x,row_tag,tags),adt_dtl_schema)

# COMMAND ----------

from pyspark.sql.functions import input_file_name,col, udf, explode
def invoke_adt_dtl_process(directory_path):
  return read_from_directory_with_filename(directory_path)\
         .withColumn("parse",explode(getMultAdtDtlUdf(row_tag,tags)(col('text'))))\
         .select(*["parse.*","msg_nm"])
         
         