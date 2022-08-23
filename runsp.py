from pyspark.sql.session import SparkSession
def main():
    spark = SparkSession.builder.appName('temp').getOrCreate()
    print(spark)
    return 1

main()