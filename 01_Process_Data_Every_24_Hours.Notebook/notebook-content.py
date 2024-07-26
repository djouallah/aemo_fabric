# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c657e18b-54ce-4b82-8fd9-0907369d5c18",
# META       "default_lakehouse_name": "storage",
# META       "default_lakehouse_workspace_id": "f6bfc2ea-3a6e-48a4-980e-7b755bb08af8",
# META       "known_lakehouses": [
# META         {
# META           "id": "c657e18b-54ce-4b82-8fd9-0907369d5c18"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **<mark>Download from the web</mark>**

# PARAMETERS CELL ********************

Nbr_Files_to_Download = 60

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import re ,shutil,glob
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
    filelist =sorted(filelist_unique, reverse=True)
  
    ### Read from existing log
    df = ds.dataset(Path + "_log/log.parquet").to_table().to_pandas()     
    file_loaded= df['file'].unique()
    current = file_loaded.tolist()
    files_to_upload = list(set(filelist) - set(current))
    files_to_upload = list(dict.fromkeys(files_to_upload))[:x] 
    print(str(len(files_to_upload)) + ' New File Loaded')
    if len(files_to_upload) != 0 :
      for x in files_to_upload:
           with requests.get(url+x, stream=True) as resp:
            if resp.ok:
              with open(f"{Path}/{x}", "wb") as f:
               for chunk in resp.iter_content(chunk_size=4096):
                f.write(chunk)
      
    #print(log)
    L=[os.path.basename(x) for x in glob.glob(Path+'*.zip')]
    log = pd.DataFrame({'file':L})
    log_tb=pa.Table.from_pandas(log,preserve_index=False)
    #print(log_tb)
    log_schema = pa.schema([pa.field('file', pa.string())])
    log_tb=log_tb.cast(target_schema=log_schema)
    pq.write_table(log_tb,Path+"_log/log.parquet")
    return "done"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

download("https://data.wa.aemo.com.au/public/market-data/wemde/facilityScada/previous/","/lakehouse/default/Files/0_Source/WA/facilityScada/",Nbr_Files_to_Download)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

download("https://nemweb.com.au/Reports/Current/Daily_Reports/","/lakehouse/default/Files/0_Source/ARCHIVE/Daily_Reports/",Nbr_Files_to_Download)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<mark>Unzip</mark>**

# CELL ********************

import pandas as pd
import multiprocessing
from shutil import unpack_archive
import os
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow as pa
def uncompress(x):
        unpack_archive(str(Source+x), str(Destination), 'zip')
def unzip(Source, Destination):
    
    #run only once to create an empty log file
    if not os.path.exists(Destination):
      os.makedirs(Destination, exist_ok=True) 
      os.makedirs(Destination+"_log", exist_ok=True)
      log_tb = pa.Table.from_pylist( ['x'], schema=pa.schema({ "file" : pa.string()}))
      pq.write_table(log_tb,Destination+"_log/log.parquet")  
    # check zip files
    try:
     df = ds.dataset(Source + "_log/log.parquet").to_table().to_pandas()     
     filelist_unique= df['file'].unique()
     filelist=filelist_unique.tolist()
    except: 
     filelist=[os.path.basename(x) for x in glob.glob(Source+'*.zip')]
    filelist =sorted(filelist, reverse=True)
    ### checl the unzipped files already
    df = ds.dataset(Destination + "_log/log.parquet").to_table().to_pandas()     
    file_loaded= df['file'].unique()
    current = file_loaded.tolist()
    files_to_upload = list(set(filelist) - set(current))
    files_to_upload = list(dict.fromkeys(files_to_upload))[:Nbr_Files_to_Download] 
    #unzip only the delta
    print(str(len(files_to_upload)) + ' New File uncompressed')
    if len(files_to_upload) != 0 :
      with multiprocessing.Pool() as pool:
       for _ in pool.imap_unordered(uncompress, files_to_upload, chunksize=1):
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

#zip for previous years data stored in cloudflare, getting data using a shortcut
Source = "/lakehouse/default/Files/aemo/"
Destination = "/lakehouse/default/Files/1_Transform/0/ARCHIVE/Daily_Reports/"
unzip(Source,Destination)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Source = "/lakehouse/default/Files/0_Source/ARCHIVE/Daily_Reports/"
Destination = "/lakehouse/default/Files/1_Transform/0/ARCHIVE/Daily_Reports/"
unzip(Source,Destination)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Source = "/lakehouse/default/Files/0_Source/WA/facilityScada/"
Destination = "/lakehouse/default/Files/1_Transform/0/WA/facilityScada/"
unzip(Source,Destination)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<mark>Transformation</mark>**

# CELL ********************

Path = "/lakehouse/default/Files/1_Transform/0/ARCHIVE/Daily_Reports/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# _**SCADA**_

# CELL ********************

from pyspark.sql import functions as f
import pandas as pd
import numpy as np
import time
def extract_scada(files_to_upload_full_Path): 
 user_schema="""
 I  STRING,UNIT  STRING,XX  STRING,VERSION  STRING,SETTLEMENTDATE  STRING,RUNNO  STRING,DUID  STRING,
 INTERVENTION  STRING,DISPATCHMODE  STRING,AGCSTATUS  STRING,INITIALMW  STRING,TOTALCLEARED  STRING,RAMPDOWNRATE  STRING,
 RAMPUPRATE  STRING,LOWER5MIN  STRING,LOWER60SEC  STRING,LOWER6SEC  STRING,RAISE5MIN  STRING,RAISE60SEC  STRING,RAISE6SEC  STRING,
 MARGINAL5MINVALUE  STRING,MARGINAL60SECVALUE  STRING,MARGINAL6SECVALUE  STRING,MARGINALVALUE  STRING,VIOLATION5MINDEGREE  STRING,
 VIOLATION60SECDEGREE  STRING,VIOLATION6SECDEGREE  STRING,VIOLATIONDEGREE  STRING,LOWERREG  STRING,RAISEREG  STRING,AVAILABILITY  STRING,
 RAISE6SECFLAGS  STRING,RAISE60SECFLAGS  STRING,RAISE5MINFLAGS  STRING,RAISEREGFLAGS  STRING,LOWER6SECFLAGS  STRING,LOWER60SECFLAGS STRING,
 LOWER5MINFLAGS  STRING,LOWERREGFLAGS  STRING,RAISEREGAVAILABILITY  STRING,RAISEREGENABLEMENTMAX  STRING,RAISEREGENABLEMENTMIN  STRING,
 LOWERREGAVAILABILITY  STRING,LOWERREGENABLEMENTMAX  STRING,LOWERREGENABLEMENTMIN  STRING,RAISE6SECACTUALAVAILABILITY  STRING,
 RAISE60SECACTUALAVAILABILITY  STRING,RAISE5MINACTUALAVAILABILITY  STRING,RAISEREGACTUALAVAILABILITY  STRING,
 LOWER6SECACTUALAVAILABILITY  STRING,LOWER60SECACTUALAVAILABILITY  STRING,LOWER5MINACTUALAVAILABILITY  STRING,
 LOWERREGACTUALAVAILABILITY  STRING,transactionId STRING
 """

 df = spark.read.format("csv")\
     .option("header","true") \
     .schema(user_schema)\
     .load(files_to_upload_full_Path)\
     .filter("unit='DUNIT' and version =3 and I='D'")\
     .drop('xx')\
     .drop('I')\
     .withColumn('SETTLEMENTDATE',f.to_timestamp('SETTLEMENTDATE','yyyy/MM/dd HH:mm:ss'))\
     .withColumn("file", f.regexp_extract(f.input_file_name(), r"Daily_Reports\/([^\W'\.']+\.CSV)", 1))\
     .withColumn("PRIORITY", f.lit(1))
 df_cols = list(set(df.columns) - {'SETTLEMENTDATE','DUID','file','UNIT','transactionId','PRIORITY'})
 for col_name in df_cols:
    df = df.withColumn(col_name, f.col(col_name).cast('double'))  
 df.createOrReplaceTempView("xx")
 df =spark.sql('select *,cast(SETTLEMENTDATE as date) as DATE, extract(year from SETTLEMENTDATE) as year  from xx')
 return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *
if spark.catalog.tableExists("scada"):
   x = spark.read.format("delta").load("Tables/scada").select('file').distinct().toPandas()
   existing_files=x['file'].tolist()
else:
    existing_files=[]

df = pd.read_parquet(Path+"_log/log.parquet")
df = df.replace(to_replace='None', value=np.nan).dropna()
list_files = df['file'].tolist()
filelist_csv = [w.replace('.zip', '.CSV') for w in list_files]
files_to_upload = list(set(filelist_csv) - set(existing_files))
files_to_upload = list(dict.fromkeys(files_to_upload))
files_to_upload_full_Path = ["Files/1_Transform/0/ARCHIVE/Daily_Reports/" + i for i in files_to_upload]
print(len(files_to_upload_full_Path))
########################### Write Data ##########################################################
if len(files_to_upload_full_Path) >0 :
 dfnew = extract_scada(files_to_upload_full_Path)
 dfnew.write.mode("append").format("delta").partitionBy("year").saveAsTable("scada")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# _**PRICE**_

# CELL ********************

from pyspark.sql import functions as f
def extract_price(files_to_upload_full_Path):
 user_schema="""I STRING,UNIT STRING,xx STRING,VERSION STRING,SETTLEMENTDATE STRING,RUNNO STRING,REGIONID STRING,INTERVENTION STRING,
 RRP STRING,EEP STRING,ROP STRING,APCFLAG STRING,MARKETSUSPENDEDFLAG STRING,TOTALDEMAND STRING,DEMANDFORECAST STRING,DISPATCHABLEGENERATION STRING,DISPATCHABLELOAD STRING,
 NETINTERCHANGE STRING,EXCESSGENERATION STRING,LOWER5MINDISPATCH STRING,LOWER5MINIMPORT STRING,LOWER5MINLOCALDISPATCH STRING,LOWER5MINLOCALPRICE STRING,
 LOWER5MINLOCALREQ STRING,LOWER5MINPRICE STRING,LOWER5MINREQ STRING,LOWER5MINSUPPLYPRICE STRING,LOWER60SECDISPATCH STRING,LOWER60SECIMPORT STRING,LOWER60SECLOCALDISPATCH STRING,
 LOWER60SECLOCALPRICE STRING,LOWER60SECLOCALREQ STRING,LOWER60SECPRICE STRING,LOWER60SECREQ STRING,LOWER60SECSUPPLYPRICE STRING,LOWER6SECDISPATCH STRING,LOWER6SECIMPORT STRING,
 LOWER6SECLOCALDISPATCH STRING,LOWER6SECLOCALPRICE STRING,LOWER6SECLOCALREQ STRING,LOWER6SECPRICE STRING,LOWER6SECREQ STRING,LOWER6SECSUPPLYPRICE STRING,RAISE5MINDISPATCH STRING,
 RAISE5MINIMPORT STRING,RAISE5MINLOCALDISPATCH STRING,RAISE5MINLOCALPRICE STRING,RAISE5MINLOCALREQ STRING,RAISE5MINPRICE STRING,RAISE5MINREQ STRING,RAISE5MINSUPPLYPRICE STRING,
 RAISE60SECDISPATCH STRING,RAISE60SECIMPORT STRING,RAISE60SECLOCALDISPATCH STRING,RAISE60SECLOCALPRICE STRING,RAISE60SECLOCALREQ STRING,RAISE60SECPRICE STRING,RAISE60SECREQ STRING,
 RAISE60SECSUPPLYPRICE STRING,RAISE6SECDISPATCH STRING,RAISE6SECIMPORT STRING,RAISE6SECLOCALDISPATCH STRING,RAISE6SECLOCALPRICE STRING,RAISE6SECLOCALREQ STRING,RAISE6SECPRICE STRING,
 RAISE6SECREQ STRING,RAISE6SECSUPPLYPRICE STRING,AGGREGATEDISPATCHERROR STRING,AVAILABLEGENERATION STRING,AVAILABLELOAD STRING,INITIALSUPPLY STRING,CLEAREDSUPPLY STRING,
 LOWERREGIMPORT STRING,LOWERREGLOCALDISPATCH STRING,LOWERREGLOCALREQ STRING,LOWERREGREQ STRING,RAISEREGIMPORT STRING,RAISEREGLOCALDISPATCH STRING,RAISEREGLOCALREQ STRING,RAISEREGREQ STRING,
 RAISE5MINLOCALVIOLATION STRING,RAISEREGLOCALVIOLATION STRING,RAISE60SECLOCALVIOLATION STRING,RAISE6SECLOCALVIOLATION STRING,LOWER5MINLOCALVIOLATION STRING,LOWERREGLOCALVIOLATION STRING,
 LOWER60SECLOCALVIOLATION STRING,LOWER6SECLOCALVIOLATION STRING,RAISE5MINVIOLATION STRING,RAISEREGVIOLATION STRING,RAISE60SECVIOLATION STRING,RAISE6SECVIOLATION STRING,
 LOWER5MINVIOLATION STRING,LOWERREGVIOLATION STRING,LOWER60SECVIOLATION STRING,LOWER6SECVIOLATION STRING,RAISE6SECRRP STRING,RAISE6SECROP STRING,RAISE6SECAPCFLAG STRING,RAISE60SECRRP STRING,
 RAISE60SECROP STRING,RAISE60SECAPCFLAG STRING,RAISE5MINRRP STRING,RAISE5MINROP STRING,RAISE5MINAPCFLAG STRING,RAISEREGRRP STRING,RAISEREGROP STRING,RAISEREGAPCFLAG STRING,LOWER6SECRRP STRING,
 LOWER6SECROP STRING,LOWER6SECAPCFLAG STRING,LOWER60SECRRP STRING,LOWER60SECROP STRING,LOWER60SECAPCFLAG STRING,LOWER5MINRRP STRING,LOWER5MINROP STRING,LOWER5MINAPCFLAG STRING,
 LOWERREGRRP STRING,LOWERREGROP STRING,LOWERREGAPCFLAG STRING,RAISE6SECACTUALAVAILABILITY STRING,RAISE60SECACTUALAVAILABILITY STRING,RAISE5MINACTUALAVAILABILITY STRING,
 RAISEREGACTUALAVAILABILITY STRING,LOWER6SECACTUALAVAILABILITY STRING,LOWER60SECACTUALAVAILABILITY STRING,LOWER5MINACTUALAVAILABILITY STRING,LOWERREGACTUALAVAILABILITY STRING,
 LORSURPLUS STRING,LRCSURPLUS STRING"""
 df = spark.read.format("csv")\
     .option("header","true") \
     .schema(user_schema)\
     .load(files_to_upload_full_Path)\
     .filter("unit='DREGION' and VERSION =3 and I='D'")\
     .drop('xx')\
     .drop('I')\
     .withColumn('SETTLEMENTDATE',f.to_timestamp('SETTLEMENTDATE','yyyy/MM/dd HH:mm:ss'))\
     .withColumn("file", f.regexp_extract(f.input_file_name(), r"Daily_Reports\/([^\W'\.']+\.CSV)", 1))\
     .withColumn("PRIORITY", f.lit(1))
 df_cols = list(set(df.columns) - {'SETTLEMENTDATE','DUID','file','REGIONID','UNIT','PRIORITY'})
 for col_name in df_cols:
    df = df.withColumn(col_name, f.col(col_name).cast('double'))  
 df.createOrReplaceTempView("xx")
 df =spark.sql('select *,cast(SETTLEMENTDATE as date) as DATE, extract(year from SETTLEMENTDATE) as year  from xx')
 return df   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists("price"):
   existing_files = spark.read.format("delta").load("Tables/price").select('file').distinct().toPandas()['file'].tolist()
else:
    existing_files=[]
#print(len(existing_files))

df = pd.read_parquet(Path+"_log/log.parquet")
df = df.replace(to_replace='None', value=np.nan).dropna()
list_files = df['file'].tolist()
filelist_csv = [w.replace('.zip', '.CSV') for w in list_files]
files_to_upload = list(set(filelist_csv) - set(existing_files))
files_to_upload = list(dict.fromkeys(files_to_upload))
files_to_upload_full_Path = ["Files/1_Transform/0/ARCHIVE/Daily_Reports/" + i for i in files_to_upload]
print(len(files_to_upload_full_Path))
########################### Write Data ##########################################################
if len(files_to_upload_full_Path) >0 :
 dfnew = extract_price(files_to_upload_full_Path)
 dfnew.write.mode("append").format("delta").saveAsTable("price")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# _**WA SCADA**_

# CELL ********************

Path_WA = "/lakehouse/default/Files/1_Transform/0/WA/facilityScada/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as f
def extract_scada_wa(files_to_upload_full_Path):
 df= spark.read.json('Files/1_Transform/0/WA/facilityScada')
 xx=df.select("transactionId",f.explode("data.facilityScadaDispatchIntervals"))\
       .select("transactionId",\
        f.col("col.code").alias("DUID"), \
        f.to_timestamp(f.col("col.dispatchInterval"),"yyyy-MM-dd'T'HH:mm:ssXXX").alias("SETTLEMENTDATE"),\
        f.col("col.quantity").alias("INITIALMW")\
        )\
  .withColumn("INTERVENTION", f.lit(0.0))\
  .withColumn("PRIORITY", f.lit(1))\
  .withColumn("file", f.regexp_extract(f.input_file_name(), r"[^\/]*$", 0))
 xx.createOrReplaceTempView("xxx")
 df =spark.sql('select *,cast(SETTLEMENTDATE as date) as DATE, extract(year from SETTLEMENTDATE) as year  from xxx')
 return df 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import numpy as np
import time
from delta.tables import *
if spark.catalog.tableExists("scada"):
   x = spark.sql(" select distinct file from scada where transactionId is not null ").toPandas()['file'].tolist()
   existing_files = [w.replace('-', '') for w in x]
else:
    existing_files=[]
#print(existing_files)

df = pd.read_parquet(Path_WA+"_log/log.parquet")
df = df.replace(to_replace='None', value=np.nan).dropna()
list_files = df['file'].tolist()
filelist_json = [w.replace('FacilityScada', 'SCADA') for w in list_files]
filelist_json = [w.replace('.zip', '.json') for w in filelist_json]
#print(filelist_json)
files_to_upload = list(set(filelist_json) - set(existing_files))
files_to_upload = list(dict.fromkeys(files_to_upload))
files_to_upload_full_Path = ["Files/1_Transform/0/WA/facilityScada/" + i for i in files_to_upload]
print(len(files_to_upload_full_Path))
########################### Write Data ##########################################################
if len(files_to_upload_full_Path) > 0 :
 dfnew = extract_scada_wa(files_to_upload_full_Path)
 dfnew.write.mode("append").format("delta").partitionBy("year").saveAsTable("scada")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# _<mark>**Update Dimension Table**</mark>_

# CELL ********************

!pip -q install duckdb
import requests
import duckdb
DUID_Path = "/lakehouse/default/Files/0_Source/Dimensions/DUID/"
import pathlib
pathlib.Path(DUID_Path).mkdir(parents=True, exist_ok=True)
url = "https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/NEM-Registration-and-Exemption-List.xls"
s = requests.Session()
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'}
r = s.get(url,headers=headers)
r.content
output = open(DUID_Path+"NEM-Registration-and-Exemption-List.xls", 'wb')
output.write(r.content)
output.close()
duckdb.sql(f"""
INSTALL spatial;
LOAD spatial;
SELECT Region,DUID,first("Fuel Source - Descriptor") as FuelSourceDescriptor,first(Participant) as Participant
FROM st_read('{DUID_Path}NEM-Registration-and-Exemption-List.xls', layer = 'PU and Scheduled Loads',open_options = ['HEADERS=FORCE'])
group by all
""").to_table('DUID')

import requests
dls = "https://data.wa.aemo.com.au/datafiles/post-facilities/facilities.csv"
resp = requests.get(dls)
output = open(DUID_Path+"facilities_WA.csv", 'wb')
output.write(resp.content)
output.close()

duckdb.sql(f""" select 'WA1' as Region  , "Facility Code" as DUID ,"Participant Name" as Participant from read_csv_auto('{DUID_Path}facilities_WA.csv')""").to_view('x')

duckdb.sql("""select x.Region,x.DUID, TECHNOLOGY as FuelSourceDescriptor,Participant from x
left join (select * FROM read_csv_auto('https://github.com/djouallah/aemo_fabric/raw/main/WA_ENERGY.csv',header=1)) as z
on x.duid=z.duid """).to_view('DUID_WA')

df_duid=duckdb.sql(f""" with xx as (select * from DUID union BY NAME select * from DUID_WA)
              select trim(DUID) as DUID,min(Region) as Region, min(FuelSourceDescriptor) as FuelSourceDescriptor,min(Participant) as Participant from xx group by all
              """).df()
df=spark.createDataFrame(df_duid)
df.write.mode("overwrite").format("delta").saveAsTable("duid")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not spark.catalog.tableExists("states"):
  import duckdb
  duckdb.sql("""
  create or replace table states(RegionID varchar, States varchar) ;
  insert into states values
    ('WA1' , 'Western Australia') ,
    ('QLD1' , 'Queensland')  ,
    ('NSW1' , 'New South Walles')  ,
    ('TAS1' , 'Tasmania')  ,
    ('SA1' , 'South Australia')  ,
    ('VIC1' , 'Victoria')  
    """)
  df= duckdb.sql('from states').df()
  spark.createDataFrame(df).write.mode("overwrite").format("delta").saveAsTable("states")
  df=duckdb.sql(""" SELECT cast(unnest(generate_series(cast ('2018-04-01' as date), cast('2024-12-31' as date), interval 1 day)) as date) as date,
           EXTRACT(year from date) as year,
           EXTRACT(month from date) as month
           """).df()

  spark.createDataFrame(df).write.mode("overwrite").format("delta").saveAsTable("calendar")
  df=duckdb.sql(""" SELECT unnest(generate_series(cast ('2018-04-01' as date), cast('2024-12-31' as date), interval 5 minute)) as SETTLEMENTDATE,
           strftime(SETTLEMENTDATE, '%I:%M:%S %p') as time,
           cast(SETTLEMENTDATE as date ) as date,
           EXTRACT(year from date) as year,
           EXTRACT(month from date) as month
           """).df()
  spark.createDataFrame(df).write.mode("overwrite").format("delta").saveAsTable("mstdatetime")
else:
  print("Table exists")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# _<mark>**Vaccum Tables**</mark>_

# CELL ********************

for x in ['scada','price','duid']:
    spark.sql(f'VACUUM {x}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
