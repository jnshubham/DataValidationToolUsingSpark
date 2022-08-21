import argparse
from pyspark.sql.session import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import row_number, coalesce, lit, when
from pyspark.sql import DataFrame
import os
import sys

def setConfig():
    os.environ['HADOOP_HOME'] = 'C:\\TestData\\'
    sys.path.append('C:\\TestData\\')
    driverPath = 'C:\\TestData\\mssql-jdbc-11.2.0.jre8.jar;:C:\\TestData\\mssql-jdbc_auth-11.2.0.x64.dll;'
    return driverPath


def getData(url: str, table: str, spark: SparkSession) -> DataFrame:
    df = spark.read.format("jdbc") \
        .option("url", url) \
        .option("dbtable", table) \
        .option("driver", 'com.microsoft.sqlserver.jdbc.SQLServerDriver').load()
    return df


def saveData(df: DataFrame, path: str, **kwargs):
    df.csv(path,kwargs)

def compareData(sdf: DataFrame, tdf: DataFrame, args: dict):
    eColumns = map(lambda x: x.strip(), args['excludedColumns'].split(','))
    kColumns = map(lambda x: x.strip(),args['keyColumns'].split(','))
    rColumns = [col for col in sdf.columns if col not in eColumns]
    
    dfpre = sdf.select(*rColumns).\
              withColumn('rowid',
                          row_number().over(Window().partitionBy(*kColumns).orderBy(*kColumns)))
    dfpost = tdf.select(*rColumns).\
              withColumn('rowid',
                          row_number().over(Window().partitionBy(*kColumns).orderBy(*kColumns)))
    
    
    dfComb = dfpost.join(dfpre, on='rowid', how='inner').\
        select(*[df[col].alias(f'{col}_{alias}') if alias in ('post','pre') else (coalesce((df[col]==alias[col]).cast('string'), when((df[col].isNull()) & (alias[col].isNull()),lit('true')).otherwise(lit('false')))).alias(col) for col in rColumns for df,alias in [(dfpost,'post'),(dfpre,'pre'),(dfpost,dfpre) ]]+['rowid'])
    
    df_combCached = dfComb.persist()
    df_combCached.count()

    mismC = []
    for col in rColumns:
        if(df_combCached.select(col).distinct().count()>1):
            mismC.append(col)

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
    
    return dfFinal
    

def main(args):
    driverPath = setConfig()
    
    #Generate Local Spark Session
    spark = SparkSession \
        .builder \
        .appName("QA Validation Report Generation") \
        .config("spark.driver.extraClassPath",driverPath) \
        .getOrCreate()
    
    #.config("spark.driver.extraClassPath",'C:\\TestData\\mssql-jdbc_auth-11.2.0.x64.dll') \

    sourceURL = 'jdbc:sqlserver://{sourceURL};databaseName={sourceDatabase};username={sourceUser};password={sourcePassword};'.format(**vars(args))
    targetURL = 'jdbc:sqlserver://{targetURL};databaseName={targetDatabase};username={targetUser};password={targetPassword};'.format(**vars(args))
    
    #Get Source Data
    sdf = getData(sourceURL, args.sourceTable, spark)
    tdf = getData(targetURL, args.targetTable, spark)

    fdf = compareData(sdf, tdf, vars(args))
    #url = 'jdbc:sqlserver://GMWCNSQLV00288.gdc0.chevron.net\SQL02;databaseName=CREDIT_INT_T2;integratedSecurity=true;trusted_connection=true'
    #url = 'jdbc:sqlserver://cashappcredit-dev2-cvx.database.windows.net;databaseName=cashappcredit-dev2-cvx;username=dbadmin-cashappcredit-dev2;password=b6sJkWzqYBnXT1kvXeHtwQuCbCBQSsc9;'


    

def getArgs():
    parser = argparse.ArgumentParser(description='Validation Framework')
    parser.add_argument('s','sourceURL',help='Contains Source URL')
    parser.add_argument('t','targetURL',help='Contains Target URL')
    parser.add_argument('u','sourceUser',help='Contains Source User')
    parser.add_argument('v','targetUser',help='Contains Target User')
    parser.add_argument('p','sourcePassword',help='Contains Source Password')
    parser.add_argument('q','targetPassword',help='Contains Target Password')
    parser.add_argument('d','sourceDatabase',help='Contains Source Database')
    parser.add_argument('f','targetDatabase',help='Contains Target Database')
    parser.add_argument('a','sourceTable',help='Contains Source Table')
    parser.add_argument('b','targetTable',help='Contains Target Table')
    parser.add_argument('k','keyColumns',help='Contains Key Columns')
    parser.add_argument('e','excludedColumns',help='Contains Columns To Exclude')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = getArgs()
    main(args)
