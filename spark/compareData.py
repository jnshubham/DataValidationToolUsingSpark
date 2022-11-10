import argparse
from pyspark.sql.session import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import row_number, coalesce, lit, when, concat_ws, col
from pyspark.sql import DataFrame
import os
import sys
import pandas
import datetime

def setConfig(sourceTable):
    filePath = os.path.abspath(os.getcwd())
    os.environ['HADOOP_HOME'] = filePath
    sys.path.append(filePath)
    driverPath = os.path.join(filePath, 'lib','*;')
    outputPath = os.path.join(filePath, 'output', sourceTable, f'{int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))}','filename')
    os.makedirs(outputPath.replace('filename',''))
    return driverPath, outputPath


def getData(url: str, table: str, spark: SparkSession, filterCondition: str, driver: str) -> DataFrame:
    print(url)
    print(table)
    df = spark.read.format("jdbc") \
        .option("url", url) \
        .option("dbtable", table) \
        .option("driver", driver).load()
    if(filterCondition):
        df.createOrReplaceTempView('table')
        df = spark.sql(f'select * from table where {filterCondition}')

    return df


def saveData(df: DataFrame, path: str, **kwargs):
    df.write.csv(path,**kwargs)

def saveDataPandas(df: DataFrame, path: str, **kwargs):
    pdf = df.toPandas()
    pdf.to_csv(path, **kwargs)

def compareData(sdf: DataFrame, tdf: DataFrame, outputPath: str, args: dict) -> DataFrame:
    sdf = sdf.dropDuplicates()
    tdf = tdf.dropDuplicates()
    eColumns = list(map(lambda x: x.strip(), args['excludedColumns'].split(',')))
    kColumns = list(map(lambda x: x.strip(),args['keyColumns'].split(',')))
    rColumns = [col for col in sdf.columns if col not in eColumns]
    
    dfpre = sdf.select(*rColumns).\
              withColumn('rowid', concat_ws(',',*kColumns))
    dfpost = tdf.select(*rColumns).\
              withColumn('rowid', concat_ws(',',*kColumns))
    
    
    dfComb = dfpost.join(dfpre, on='rowid', how='inner').\
        select(*[df[col].alias(f'{col}_{alias}') if alias in ('post','pre') else (coalesce((df[col]==alias[col]).cast('string'), when((df[col].isNull()) & (alias[col].isNull()),lit('true')).otherwise(lit('false')))).alias(col) for col in rColumns for df,alias in [(dfpost,'post'),(dfpre,'pre'),(dfpost,dfpre) ]]+['rowid'])
    
    df_combCached = dfComb.persist()
    #df_combCached = dfComb
    df_combCached.count()

    mismC = []
    for cm in rColumns:
        if(df_combCached.select(cm).distinct().count()>1):
            mismC.append(cm)

    firstFlag = True
    for clm in rColumns:
        key = list(map(lambda x: x+'_pre',kColumns))
        if(firstFlag):
            dfFinal = df_combCached.filter(col(clm)=='false').\
            select(concat_ws(',',*key).cast('string').\
                   alias('BusinessKey('+','.join(kColumns)+')'),
            lit(clm).cast('string').alias('ColumnName'),
            col(clm+'_pre').cast('string').alias('pre'),
            col(clm+'_post').cast('string').alias('post'),
            when(col(clm+'_pre').cast('int').isNotNull() & col(clm+'_post').cast('int').isNotNull(),\
                 (col(clm+'_pre')-col(clm+'_post')).cast('string')).otherwise(lit('Differences found')).alias('Comments')
            )
            firstFlag = False
        else:
            dfFinal = dfFinal.union(df_combCached.filter(col(clm)=='false').\
            select(concat_ws(',',*key).cast('string').\
                   alias('BusinessKey('+','.join(kColumns)+')'),
            lit(clm).cast('string').alias('ColumnName'),
            col(clm+'_pre').cast('string').alias('pre'),
            col(clm+'_post').cast('string').alias('post'),
            when(col(clm+'_pre').cast('int').isNotNull() & col(clm+'_post').cast('int').isNotNull(),\
                 (col(clm+'_pre')-col(clm+'_post')).cast('string')).otherwise(lit('Differences found')).alias('Comments')
            ))
            
            
    #Get rows Missing in pre
    dfMissingRowsPre = dfpost.join(dfpre, on='rowid', how='leftanti').\
                        select(concat_ws(',',*[dfpost[cl] for cl in kColumns]).cast('string').\
                               alias('BusinessKey('+','.join(kColumns)+')'),
                        lit('Row NotFound in pre').cast('string').alias('ColumnName'),
                        lit('').cast('string').alias('pre'),
                        lit('').cast('string').alias('post'),
                        lit('Row not found in pre').alias('Comments')
                        )
    
    #Get rows Missing in pre    
    dfMissingRowsPost = dfpre.join(dfpost, on='rowid', how='leftanti').\
                        select(concat_ws(',',*[dfpre[cl] for cl in kColumns]).cast('string').\
                               alias('BusinessKey('+','.join(kColumns)+')'),
                        lit('Row NotFound in post').cast('string').alias('ColumnName'),
                        lit('').cast('string').alias('pre'),
                        lit('').cast('string').alias('post'),
                        lit('Row not found in post').alias('Comments')
                        )
    
    dfFinal = dfFinal.union(dfMissingRowsPost).union(dfMissingRowsPre)
    if(dfFinal.count()>0):
        saveDataPandas(dfFinal, outputPath.replace('filename','dataComparision_FAILED.csv'), index=False, header=True, mode='w+')
    else:
        saveDataPandas(dfFinal, outputPath.replace('filename','dataComparision_SUCCESS.csv'), index=False, header=True, mode='w+')

    
    

def countValidation(sdf: DataFrame, tdf: DataFrame, outputPath: str):
    sourceCount = sdf.count()
    targetCount = tdf.count()
    sdf = sdf.dropDuplicates()
    tdf = tdf.dropDuplicates()
    source2Count = sdf.count()
    target2Count = tdf.count()
    
    countResult = ''
    if(sourceCount==targetCount and source2Count==target2Count):
        countResult = countResult + f'Before removing duplicates \nSource Count: {str(sourceCount)} \nTarget Count: {str(targetCount)} \nAfter removing duplicates \nSource Count: {str(source2Count)} \nTarget Count: {str(target2Count)} \nCount Validation Passes.'
        outputPath = outputPath.replace('filename','countComparision_SUCCESS.txt')
    elif(sourceCount==targetCount):
        countResult = countResult + f'Before removing duplicates \nSource Count: {str(sourceCount)} \nTarget Count: {str(targetCount)} \nAfter removing duplicates \nSource Count: {str(source2Count)} \nTarget Count: {str(target2Count)} \nCount Validation Failed.'
        outputPath = outputPath.replace('filename','countComparision_FAILED.txt')
    elif(source2Count==target2Count):
        countResult = countResult + f'Before removing duplicates \nSource Count: {str(sourceCount)} \nTarget Count: {str(targetCount)} \nAfter removing duplicates \nSource Count: {str(source2Count)} \nTarget Count: {str(target2Count)} \nCount Validation Failed but will continue further testing.'
        outputPath = outputPath.replace('filename','countComparision_FAILED.txt')
    else:
        countResult = countResult + f'Before removing duplicates \nSource Count: {str(sourceCount)} \nTarget Count: {str(targetCount)} \nAfter removing duplicates \nSource Count: {str(source2Count)} \nTarget Count: {str(target2Count)} \nCount Validation Failed.'
        outputPath = outputPath.replace('filename','countComparision_FAILED.txt')
    
    with open(outputPath,'w+') as t:
            t.write(countResult)
        
def DataTypeValidation(surl: str, turl: str, outputPath:str, stable: str, ttable: str, spark: SparkSession, driver: str) -> DataFrame:
    dtQuery = '''SELECT COLUMN_NAME, DATA_TYPE+'('+ case when CHARACTER_MAXIMUM_LENGTH is not null then cast(CHARACTER_MAXIMUM_LENGTH as varchar) else
case when NUMERIC_PRECISION is not null then cast(NUMERIC_PRECISION as varchar) +', '+cast(NUMERIC_SCALE as varchar)  else cast(DATETIME_PRECISION as varchar)  end end  +')' as datatype, ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE table_schema='{0}' and 
TABLE_NAME = '{1}'
'''

    sdtQuery = dtQuery.format(stable.split('.')[0], stable.split('.')[1])
    tdtQuery = dtQuery.format(ttable.split('.')[0], ttable.split('.')[1])

    sDTdf = spark.read.format("jdbc") \
        .option("url", surl) \
        .option("query", sdtQuery) \
        .option("driver", 'com.microsoft.sqlserver.jdbc.SQLServerDriver').load()
        
    tDTdf = spark.read.format("jdbc") \
        .option("url", turl) \
        .option("query", tdtQuery) \
        .option("driver", 'com.microsoft.sqlserver.jdbc.SQLServerDriver').load()
        
    fDTdf = sDTdf.join(tDTdf, on='COLUMN_NAME', how='full').\
        select(sDTdf['COLUMN_NAME'].alias('columnName_pre'), tDTdf['COLUMN_NAME'].alias('columnName_post'), sDTdf['datatype'].alias('datatype_pre'), tDTdf['datatype'].alias('datatype_post'), sDTdf['ORDINAL_POSITION'].alias('ordinalPosition_pre'), tDTdf['ORDINAL_POSITION'].alias('ordinalPosition_post'),
        ((sDTdf['ORDINAL_POSITION']==tDTdf['ORDINAL_POSITION']) & (sDTdf['COLUMN_NAME']==tDTdf['COLUMN_NAME']) & (sDTdf['datatype']==tDTdf['datatype'])).alias('MatchResult'))
        
    if(fDTdf.filter(col('MatchResult')=='false').count()>=1):
        saveDataPandas(fDTdf, outputPath.replace('filename','dataTypeComparision_FAILED.csv'), index=False, header=True, mode='w+')
    else:
        saveDataPandas(fDTdf, outputPath.replace('filename','dataTypeComparision_SUCCESS.csv'), index=False, header=True, mode='w+')
        
        


def main(args):
    driverPath, outputPath = setConfig(args.sourceTable)
    #Generate Local Spark Session
    spark = SparkSession \
        .builder \
        .appName("QA Validation Report Generation") \
        .config("spark.driver.extraClassPath",driverPath) \
        .config("spark.driver.memory",'60g') \
        .getOrCreate()
    
    #.config("spark.sql.execution.arrow.enabled-", "true") \
    #print(spark.sparkContext._conf.getAll())
    #.config("spark.driver.extraClassPath",'C:\\TestData\\mssql-jdbc_auth-11.2.0.x64.dll') \

    # sourceURL = 'jdbc:sqlserver://{sourceURL};databaseName={sourceDatabase};username={sourceUser};password={sourcePassword};'.format(**vars(args))
    # targetURL = 'jdbc:sqlserver://{targetURL};databaseName={targetDatabase};username={targetUser};password={targetPassword};'.format(**vars(args))
    sourceURL = args.sourceURL
    targetURL = args.targetURL
    
    #Get Source Data
    sdf = getData(sourceURL, args.sourceTable, spark, args.filterCondition, args.sourceDriver)
    tdf = getData(targetURL, args.targetTable, spark, args.filterCondition, args.targetDriver)
    #saveData(fdf, outputPath.replace('comparisionType','dataValidation').replace('filename',''), header=True, mode=
    # 'overwrite')
    
    #url = 'jdbc:sqlserver://GMWCNSQLV00288.gdc0.chevron.net\SQL02;databaseName=CREDIT_INT_T2;integratedSecurity=true;trusted_connection=true'
    #url = 'jdbc:sqlserver://cashappcredit-dev2-cvx.database.windows.net;databaseName=cashappcredit-dev2-cvx;username=dbadmin-cashappcredit-dev2;password=b6sJkWzqYBnXT1kvXeHtwQuCbCBQSsc9;'

    if(args.steps=='1' or args.steps=='2' or args.steps=='0'):
        compStart = datetime.datetime.now()
        compareData(sdf, tdf, outputPath, vars(args))
        print(f'Time Taken for Data Validation:{str(datetime.datetime.now()-compStart)}')
    
    if(args.steps=='2' or args.steps=='0'):
        countstart = datetime.datetime.now()
        countValidation(sdf, tdf, outputPath)
        print(f'Time Taken for Count Validation:{str(datetime.datetime.now()-countstart)}')
        
    if(args.steps=='0'):
        dtStart = datetime.datetime.now()
        DataTypeValidation(sourceURL, targetURL, outputPath, args.sourceTable, args.targetTable, spark, args.driver)
        print(f'Time Taken for Datatype Validation:{str(datetime.datetime.now()-dtStart)}')
        


    

def getArgs():
    parser = argparse.ArgumentParser(description='Validation Framework')
    parser.add_argument('-s','--sourceURL',help='Contains Source URL')
    parser.add_argument('-t','--targetURL',help='Contains Target URL')
    parser.add_argument('-u','--sourceDriver',help='Contains Source Driver Name')
    parser.add_argument('-v','--targetDriver',help='Contains Target Driver Name')
    parser.add_argument('-a','--sourceTable',help='Contains Source Table')
    parser.add_argument('-b','--targetTable',help='Contains Target Table')
    parser.add_argument('-n','--steps',help='Which step to run 0 for all, 1 for data comparision, 2 for data and count')
    parser.add_argument('-k','--keyColumns',help='Contains Key Columns')
    parser.add_argument('-e','--excludedColumns',help='Contains Columns To Exclude', default='abracadabra')
    parser.add_argument('-l','--filterCondition',help='Filter Condition', default=False)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    start = datetime.datetime.now()
    args = getArgs()
    main(args)
    print(f'Total Time Taken:{str(datetime.datetime.now()-start)}')
