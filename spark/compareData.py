import argparse
from pyspark.sql.session import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import row_number, coalesce, lit, when, concat_ws, col
from pyspark.sql import DataFrame
import os
import sys
import pandas
import datetime

def setConfig():
    filePath = os.path.abspath(os.getcwd())
    if(sys.platform=='win32'):
        os.environ['HADOOP_HOME'] = filePath
        sys.path.append(filePath)
        driverPath = f'{filePath}\\bin\\mssql-jdbc-11.2.0.jre8.jar;:{filePath}\\bin\\mssql-jdbc_auth-11.2.0.x64.dll;'
        outputPath_pandas = f'{filePath}\\output\\{int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))}\\comparisionResult.csv'
        outputPath_spark = f'{filePath}\\output\\{int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))}'
    else:
        driverPath = f'{filePath}/bin/mssql-jdbc-11.2.0.jre8.jar;:{filePath}/bin/mssql-jdbc_auth-11.2.0.x64.dll;'
        outputPath_pandas = f'{filePath}/output/{int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))}/comparisionResult.csv'
        outputPath_spark = f'{filePath}/output/{int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))}'
    return driverPath, outputPath_pandas, outputPath_spark


def getData(url: str, table: str, spark: SparkSession, filterCondition: str) -> DataFrame:
    df = spark.read.format("jdbc") \
        .option("url", url) \
        .option("dbtable", table) \
        .option("driver", 'com.microsoft.sqlserver.jdbc.SQLServerDriver').load()
    if(filterCondition):
        df.createOrReplaceTempView('table')
        df = spark.sql(f'select * from table where {filterCondition}')
        #can remove this code
        df = df.dropDuplicates()
    return df


def saveData(df: DataFrame, path: str, **kwargs):
    df.write.csv(path,**kwargs)

def saveDataPandas(df: DataFrame, path: str, **kwargs):
    pdf = df.toPandas()
    pdf.to_csv(path, **kwargs)

def compareData(sdf: DataFrame, tdf: DataFrame, args: dict):
    eColumns = list(map(lambda x: x.strip(), args['excludedColumns'].split(',')))
    kColumns = list(map(lambda x: x.strip(),args['keyColumns'].split(',')))
    rColumns = [col for col in sdf.columns if col not in eColumns]
    print(eColumns)
    print(kColumns)
    print(rColumns)
    dfpre = sdf.select(*rColumns).\
              withColumn('rowid', concat_ws(',',*kColumns))
    dfpost = tdf.select(*rColumns).\
              withColumn('rowid', concat_ws(',',*kColumns))
    
    
    dfComb = dfpost.join(dfpre, on='rowid', how='inner').\
        select(*[df[col].alias(f'{col}_{alias}') if alias in ('post','pre') else (coalesce((df[col]==alias[col]).cast('string'), when((df[col].isNull()) & (alias[col].isNull()),lit('true')).otherwise(lit('false')))).alias(col) for col in rColumns for df,alias in [(dfpost,'post'),(dfpre,'pre'),(dfpost,dfpre) ]]+['rowid'])
    
    df_combCached = dfComb.persist()
    #df_combCached = dfComb
    print(df_combCached.count())

    mismC = []
    for cm in rColumns:
        if(df_combCached.select(cm).distinct().count()>1):
            mismC.append(cm)

    firstFlag = True
    for clm in rColumns:
        print(clm)
        key = list(map(lambda x: x+'_pre',kColumns))
        if(firstFlag):
            dfFinal = df_combCached.filter(col(clm)=='false').\
            select(concat_ws(',',*key).cast('string').\
                   alias('BusinessKey('+','.join(kColumns)+')'),
            lit(clm).cast('string').alias('ColumnName'),
            col(clm+'_pre').cast('string').alias('pre'),
            col(clm+'_post').cast('string').alias('post'),
            lit('Differences found').alias('Comments')
            )
            firstFlag = False
        else:
            dfFinal = dfFinal.union(df_combCached.filter(col(clm)=='false').\
            select(concat_ws(',',*key).cast('string').\
                   alias('BusinessKey('+','.join(kColumns)+')'),
            lit(clm).cast('string').alias('ColumnName'),
            col(clm+'_pre').cast('string').alias('pre'),
            col(clm+'_post').cast('string').alias('post'),
            lit('Differences found').alias('Comments')
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
    dfFinal.show()
    return dfFinal
    

def main(args):
    driverPath, opPandas, opSpark = setConfig()
    
    #Generate Local Spark Session
    spark = SparkSession \
        .builder \
        .appName("QA Validation Report Generation") \
        .config("spark.driver.extraClassPath",driverPath) \
        .config("spark.driver.memory",'60g') \
        .getOrCreate()
    
    #.config("spark.sql.execution.arrow.enabled-", "true") \
    print(spark.sparkContext._conf.getAll())
    #.config("spark.driver.extraClassPath",'C:\\TestData\\mssql-jdbc_auth-11.2.0.x64.dll') \

    sourceURL = 'jdbc:sqlserver://{sourceURL};databaseName={sourceDatabase};username={sourceUser};password={sourcePassword};'.format(**vars(args))
    targetURL = 'jdbc:sqlserver://{targetURL};databaseName={targetDatabase};username={targetUser};password={targetPassword};'.format(**vars(args))
    
    #Get Source Data
    sdf = getData(sourceURL, args.sourceTable, spark, args.filterCondition)
    tdf = getData(targetURL, args.targetTable, spark, args.filterCondition)

    fdf = compareData(sdf, tdf, vars(args))
    #saveData(fdf, opSpark, header=True, mode=
    # 'overwrite')
    saveDataPandas(fdf, opPandas, index=False, header=True, mode='w+')
    #url = 'jdbc:sqlserver://GMWCNSQLV00288.gdc0.chevron.net\SQL02;databaseName=CREDIT_INT_T2;integratedSecurity=true;trusted_connection=true'
    #url = 'jdbc:sqlserver://cashappcredit-dev2-cvx.database.windows.net;databaseName=cashappcredit-dev2-cvx;username=dbadmin-cashappcredit-dev2;password=b6sJkWzqYBnXT1kvXeHtwQuCbCBQSsc9;'


    

def getArgs():
    parser = argparse.ArgumentParser(description='Validation Framework')
    parser.add_argument('-s','--sourceURL',help='Contains Source URL')
    parser.add_argument('-t','--targetURL',help='Contains Target URL')
    parser.add_argument('-u','--sourceUser',help='Contains Source User')
    parser.add_argument('-v','--targetUser',help='Contains Target User')
    parser.add_argument('-p','--sourcePassword',help='Contains Source Password')
    parser.add_argument('-q','--targetPassword',help='Contains Target Password')
    parser.add_argument('-d','--sourceDatabase',help='Contains Source Database')
    parser.add_argument('-f','--targetDatabase',help='Contains Target Database')
    parser.add_argument('-a','--sourceTable',help='Contains Source Table')
    parser.add_argument('-b','--targetTable',help='Contains Target Table')
    parser.add_argument('-k','--keyColumns',help='Contains Key Columns')
    parser.add_argument('-e','--excludedColumns',help='Contains Columns To Exclude')
    parser.add_argument('-l','--filterCondition',help='Filter Condition', default=False)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    start = datetime.datetime.now()
    args = getArgs()
    main(args)
    print(f'Total Time Taken:{str(datetime.datetime.now()-start)}')
