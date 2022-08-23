# Databricks notebook source
dbutils.widgets.text('TableName', defaultValue='')
dbutils.widgets.text('SSISTableName',defaultValue='')
dbutils.widgets.text('columnsToExclude',defaultValue='')


# COMMAND ----------

database,table = dbutils.widgets.get('TableName').split('.')
ssisdatabase,ssistable = dbutils.widgets.get('SSISTableName').split('.')

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Window
import pyspark.sql.functions as f

# COMMAND ----------

url = '''jdbc:sqlserver://credit-dev2-cvx.database.windows.net:1433;database=credit-dev2-cvx;user=sql-creditcnsldtn-rwe-np@credit-dev2-cvx;password=LaYmjOMNVAdUZXZw88ghWqMuWig6JvKK;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;'''
try:
    dbutils.fs.mount(
      source = "wasbs://cashapp@energyspmintgdev.blob.core.windows.net",
      mount_point = "/mnt/cashapp",
      extra_configs = {"fs.azure.account.key.energyspmintgdev.blob.core.windows.net": 'l692FlfXkga+eubrvwROH6yHxhj+gL1i47VGVcBoEBHFD3PJp3KktfkSGe/GFDIO5AP6Env5ojqkz6BejmX/Xw=='}
    )
    dbutils.fs.mount(
      source = "wasbs://validation@energyspmintgdev.blob.core.windows.net",
      mount_point = "/mnt/energy",
      extra_configs = {"fs.azure.account.key.energyspmintgdev.blob.core.windows.net": 'l692FlfXkga+eubrvwROH6yHxhj+gL1i47VGVcBoEBHFD3PJp3KktfkSGe/GFDIO5AP6Env5ojqkz6BejmX/Xw=='}
    )
except Exception as e:
    print(e)
    pass

# COMMAND ----------

dfazure = spark.read.format('jdbc').option('url',url).option('dbtable',f'{database}.{table}').load()
dfssis = spark.read.format('jdbc').option('url',url).option('dbtable',f'{ssisdatabase}.{ssistable}').load()


# COMMAND ----------

# dfazure = dfazure.filter(f.col('CASH_PRICE_REALIZED').isNotNull())
# dfazure.count()

# COMMAND ----------

# colToAnalyze = []
# for col, dt in dfssis.dtypes:
#     if(dt in ('int')):
#         colToAnalyze.append(col)

# COMMAND ----------

# df=spark.read.parquet('/mnt/cashapp3/eod/2021-09-06/12345-12345-12345/08_job_sra_referencedata_extract/headerdetail/raextract/')
# display(df.filter(col('RiskID')=='69041099').selectExpr('totalvalue','cast(TotalValue as decimal(28,5)) as tv','cast(TotalValue as float) as tv' ))
# display(df.filter(col('RiskID')=='69040691').selectExpr('totalvalue','cast(TotalValue as decimal(28,5)) as tv','cast(TotalValue as float) as tv' ))
# display(df.filter(col('RiskID')=='69679993').selectExpr('totalvalue','cast(TotalValue as decimal(28,5)) as tv','cast(TotalValue as float) as tv' ))


# COMMAND ----------

# df_cached = dfssis.cache()
# df_cached.count()

# COMMAND ----------

# ordCol = []
# for c in colToAnalyze:
#     if(df_cached.filter(f.col(c).isNull()).count()==0):
#         ordCol.append(c)
        
        

# COMMAND ----------

ordCol=['RecordId']

# COMMAND ----------

NAColumns = dbutils.widgets.get('columnsToExclude').split(',')

# COMMAND ----------

RColumns = [col for col in dfazure.columns if col not in NAColumns]

# COMMAND ----------

dfazure = dfazure.select(*RColumns).orderBy(*RColumns).withColumn('rowid',row_number().over(Window().orderBy(*RColumns)))
dfssis = dfssis.select(*RColumns).orderBy(*RColumns).withColumn('rowid',row_number().over(Window().orderBy(*RColumns)))

# COMMAND ----------


# selCol = []
# for col in RColumns:
#     selCol.append(dfazure[col])
#     selCol.append(dfssis[col])
# #print(selCol)

dfComb = dfazure.join(dfssis,on='rowid',how='inner').select(*[df[col].alias(f'{col}_{alias}') if alias in ('azure','ssis') else (coalesce((df[col]==alias[col]).cast('string'), when((df[col].isNull()) & (alias[col].isNull()),lit('true')).otherwise(lit('false')))).alias(col) for col in RColumns for df,alias in [(dfazure,'azure'),(dfssis,'ssis'),(dfazure,dfssis) ]]+['rowid'])
df_combCached = dfComb.persist()
df_combCached.count()

# COMMAND ----------

mismC = []
for col in RColumns:
    if(df_combCached.select(col).distinct().count()>1):
        mismC.append(col)
        print(col)
#display(df_combCached)

# COMMAND ----------

import pyspark.sql.functions as f
for col in RColumns:
    print(col)
    display(df_combCached.filter(f.col(col)=='false').select(col+'_ssis',col+'_azure','rowid'))

# COMMAND ----------

# #Rows available in ssis and not in azure
# rowssis = dfssis.exceptAll(dfazure)
# rowazure = dfazure.exceptAll(dfssis)
# rowdifference = rowssis.unionAll(rowazure)

# COMMAND ----------

# display(df_combCached.filter(f.col('rowid')==416164))

# COMMAND ----------

# display(df_combCached.filter(f.col('RiskID_azure')=='66806191'))

# COMMAND ----------

# df = spark.read.parquet('/mnt/cashapp/eod/2021-09-06/12345-12345-12345/08_job_sra_referencedata_extract/headerdetail/raextract/GlobalTrading_Positions.parquet')

# COMMAND ----------

# display(df.filter(f.col('RiskID')=='66806191'))

# COMMAND ----------

# dbutils.library.installPyPI('xlsxwriter')
# dbutils.library.restartPython()

# COMMAND ----------

#%pip install xlsxwriter

# COMMAND ----------

df=spark.read.parquet('/mnt/cashapp/Credit-EOD-RA-EC/2022-01-21/masterPipelineid/jobName/12345-12345-12345/dealdetail/raextract/product.parquet')

# COMMAND ----------

display(df)

# COMMAND ----------

url = '''jdbc:sqlserver://sql-trdintcredit-dev.database.windows.net:1433;database=sqldb-trdintcredit-dev;user=ssdtAdmin@sql-trdintcredit-dev;password=Sapient@777;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;'''
try:
    dbutils.fs.mount(
      source = "wasbs://cashapp@sttrdintcreditdev2.blob.core.windows.net",
      mount_point = "/mnt/cashapp3",
      extra_configs = {"fs.azure.account.key.sttrdintcreditdev2.blob.core.windows.net": 'Lj0nlXXIqs8el6I250bFoUPd9tok1G9ync1nuygdO/XYRTYlXQRyEblwuzbec1TFN1FqwPv7wGHMqdh+IYDJwg=='}
    )
    dbutils.fs.mount(
      source = "wasbs://validation@energyspmintgdev.blob.core.windows.net",
      mount_point = "/mnt/energy",
      extra_configs = {"fs.azure.account.key.energyspmintgdev.blob.core.windows.net": 'l692FlfXkga+eubrvwROH6yHxhj+gL1i47VGVcBoEBHFD3PJp3KktfkSGe/GFDIO5AP6Env5ojqkz6BejmX/Xw=='}
    )
except Exception as e:
    print(e)
    pass

# COMMAND ----------

df = spark.read.parquet('/mnt/cashapp2/eod/2022-02-04/12345-12345-12345/09_job_sra_data_extract/headerdetail/raextract/GlobalTrading_Positions.parquet')

# COMMAND ----------

df.filter(length(col('CostTypeDescription'))>50).select('CostTypeDescription','RiskID').show(1,False)

# COMMAND ----------


