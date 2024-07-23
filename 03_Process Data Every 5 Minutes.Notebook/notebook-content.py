# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "aabe6c2b-5190-4590-859f-dcde466f9afc",
# META       "default_lakehouse_name": "storage",
# META       "default_lakehouse_workspace_id": "6353596c-e08e-46d5-b54f-d709908eade1"
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# **<mark>Download from the web</mark>**

# CELL ********************

Nbr_Files_to_Download = 700

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import re ,shutil
from urllib.request import urlopen
import os
import requests
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow as pa
def download(url,Path,x):
    #run only once to create an empty log file
    if not os.path.exists(Path):
      os.makedirs(Path, exist_ok=True) 
      os.makedirs(Path+"_log", exist_ok=True)
      log_tb = pa.Table.from_pylist( ['x'], schema=pa.schema({ "file" : pa.string()}))
      pq.write_table(log_tb,Path+"_log/log.parquet")  
    # Regex don't ask I just copy it
    result = urlopen(url).read().decode('utf-8')
    pattern = re.compile(r'[\w.]*.zip')
    filelist1 = pattern.findall(result)
    filelist_unique = dict.fromkeys(filelist1)
    filelist_sorted=sorted(filelist_unique, reverse=True)
    filelist = filelist_sorted[:x]
    ### Read from existing log
    df = ds.dataset(Path + "_log/log.parquet").to_table().to_pandas()     
    file_loaded= df['file'].unique()
    current = file_loaded.tolist()
    files_to_upload = list(set(filelist) - set(current))
    files_to_upload = list(dict.fromkeys(files_to_upload)) 
    print(str(len(files_to_upload)) + ' New File Loaded')
    if len(files_to_upload) != 0 :
      for x in files_to_upload:
           with requests.get(url+x, stream=True) as resp:
            if resp.ok:
              with open(f"{Path}/{x}", "wb") as f:
               for chunk in resp.iter_content(chunk_size=4096):
                f.write(chunk)
      existing_file = pd.DataFrame( file_loaded)
      new_file = pd.DataFrame(  files_to_upload)
      log = pd.concat ([new_file,existing_file], ignore_index=True)
      #print(log)
      log.rename(columns={0: 'file'}, inplace=True)
      log_tb=pa.Table.from_pandas(log,preserve_index=False)
      #print(log_tb)
      log_schema = pa.schema([pa.field('file', pa.string())])
      log_tb=log_tb.cast(target_schema=log_schema)
      pq.write_table(log_tb,Path+"_log/log.parquet")
      return "done"
    else:
     return "nothing to see here"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

download("http://nemweb.com.au/Reports/Current/DispatchIS_Reports/","/lakehouse/default/Files/0_Source/Current/DispatchIS_Reports/",Nbr_Files_to_Download)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

download("http://nemweb.com.au/Reports/Current/Dispatch_SCADA/","/lakehouse/default/Files/0_Source/Current/Dispatch_SCADA/",Nbr_Files_to_Download)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<mark>Unzip</mark>**

# CELL ********************

import pandas as pd
from shutil import unpack_archive
import os
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow as pa
def unzip(Source, Destination):
    #run only once to create an empty log file
    if not os.path.exists(Destination):
      os.makedirs(Destination, exist_ok=True) 
      os.makedirs(Destination+"_log", exist_ok=True)
      log_tb = pa.Table.from_pylist( ['x'], schema=pa.schema({ "file" : pa.string()}))
      pq.write_table(log_tb,Destination+"_log/log.parquet")  
    # check zip files
    df = ds.dataset(Source + "_log/log.parquet").to_table().to_pandas()     
    filelist_unique= df['file'].unique()
    filelist=filelist_unique.tolist()

    ### Read from existing log
    df = ds.dataset(Destination + "_log/log.parquet").to_table().to_pandas()     
    file_loaded= df['file'].unique()
    current = file_loaded.tolist()
    files_to_upload = list(set(filelist) - set(current))
    files_to_upload = list(dict.fromkeys(files_to_upload)) 
    print(str(len(files_to_upload)) + ' New File Loaded')
    if len(files_to_upload) != 0 :
      for x in files_to_upload:
          try:
            unpack_archive(str(Source+x), str(Destination), 'zip')
          except:
            pass
      existing_file = pd.DataFrame( file_loaded)
      new_file = pd.DataFrame(  files_to_upload)
      log = pd.concat ([new_file,existing_file], ignore_index=True)
      #print(log)
      log.rename(columns={0: 'file'}, inplace=True)
      log_tb=pa.Table.from_pandas(log,preserve_index=False)
      #print(log_tb)
      log_schema = pa.schema([pa.field('file', pa.string())])
      log_tb=log_tb.cast(target_schema=log_schema)
      pq.write_table(log_tb,Destination+"_log/log.parquet")
      return "done"
    else:
     return "nothing to see here"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

unzip("/lakehouse/default/Files/0_Source/Current/Dispatch_SCADA/","/lakehouse/default/Files/1_Transform/0/Current/Dispatch_SCADA/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

unzip("/lakehouse/default/Files/0_Source/Current/DispatchIS_Reports/","/lakehouse/default/Files/1_Transform/0/Current/DispatchIS_Reports/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<mark>Transform</mark>**

# MARKDOWN ********************

# _**SCADA**_

# CELL ********************

import pandas as pd
import numpy as np
import time
from delta.tables import *
def extract_scada(Path,files_to_upload) :
 appended_data = []
 for filename in files_to_upload:
  try:
    df = pd.read_csv(Path+filename, skiprows=1)
    df=df.dropna(how='all') #drop na
    df['SETTLEMENTDATE']= pd.to_datetime(df['SETTLEMENTDATE'])
    df['DATE']= df['SETTLEMENTDATE'].dt.date
    df = df[df.I != "C"]
    df = pd.DataFrame(df, columns=['SETTLEMENTDATE','DUID','SCADAVALUE','DATE'])
    df=df.rename(columns={"SCADAVALUE": "INITIALMW"})
    df['INTERVENTION'] = 0.0
    df['PRIORITY'] =0
    df['PRIORITY'] = df['PRIORITY'].astype(np.int32)
    df['year'] = df['SETTLEMENTDATE'].dt.year
    df['file'] = filename
    appended_data.append(df)
  except:
    pass
 appended_data = pd.concat(appended_data,ignore_index=True)
 appended_data = appended_data.replace(to_replace='None', value=np.nan).dropna()
 return appended_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

existing_files=spark.sql(""" select distinct file as file from scada where PRIORITY = 0 and year >= 2024 """).toPandas()['file'].tolist()
len(existing_files)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Path = "/lakehouse/default/Files/1_Transform/0/Current/Dispatch_SCADA/"
df = pd.read_parquet(Path+"_log/log.parquet")
df = df.replace(to_replace='None', value=np.nan).dropna()
list_files = df['file'].tolist()
filelist_csv = [w.replace('.zip', '.CSV') for w in list_files]
files_to_upload = list(set(filelist_csv) - set(existing_files))
files_to_upload = list(dict.fromkeys(files_to_upload))
print(len(files_to_upload))
########################### Write Data ##########################################################
if len(files_to_upload) >0 :
    df=spark.createDataFrame(extract_scada(Path,files_to_upload))
    #display(df)
    df.write.mode("append").format("delta").partitionBy("year").saveAsTable("scada")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# _**PRICE**_

# CELL ********************

def extract_price(Path,files_to_upload) :
 appended_data = []
 for filename in files_to_upload:
   try:
    df = pd.read_csv(Path+filename,skiprows=1,dtype=str,names=range(109),usecols=range(10))
    df.columns = df.iloc[0]
    df = df[1:]
    df=df.query('CASE_SOLUTION=="PRICE"')
    df.columns = df.iloc[0]
    df = df[1:]
    df = df.loc[:, df.columns.notnull()]
    df = pd.DataFrame(df, columns=['SETTLEMENTDATE','INTERVENTION','REGIONID','RRP'])
    df['SETTLEMENTDATE']=pd.to_datetime(df['SETTLEMENTDATE'])
    df['DATE']= df['SETTLEMENTDATE'].dt.date
    df['RRP']=pd.to_numeric(df['RRP'])
    df['INTERVENTION'] = df['INTERVENTION'].astype(np.float64)
    df['PRIORITY'] =0
    df['PRIORITY'] = df['PRIORITY'].astype(np.int32)
    df['YEAR'] = df['SETTLEMENTDATE'].dt.year
    df['file'] = filename
    appended_data.append(df)
   except:
    pass
 appended_data = pd.concat(appended_data,ignore_index=True)
 return appended_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

existing_files=spark.sql('select distinct file as file from price where PRIORITY = 0 and year= 2024').toPandas()['file'].tolist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

###########################################
Path = "/lakehouse/default/Files/1_Transform/0/Current/DispatchIS_Reports/"
df = pd.read_parquet(Path+"_log/log.parquet")
df = df.replace(to_replace='None', value=np.nan).dropna()
list_files = df['file'].tolist()
filelist_csv = [w.replace('.zip', '.CSV') for w in list_files]
files_to_upload = list(set(filelist_csv) - set(existing_files))
files_to_upload = list(dict.fromkeys(files_to_upload))
print(len(files_to_upload))
########################### Write Data ##########################################################
if len(files_to_upload) >0 :
 df=spark.createDataFrame(extract_price(Path,files_to_upload))
 df.write.mode("append").format("delta").saveAsTable("price")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>**Maintenance**</mark>

# CELL ********************

from pyspark.sql.functions import max
x = spark.sql('describe detail scada').select(max("numFiles")).head()[0]
print(x)
if x > 385 :
    spark.sql('OPTIMIZE scada ' )
    spark.sql('OPTIMIZE price' )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
