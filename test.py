import configparser
configs = configparser.ConfigParser()
configs.read('C:\\Work\\DataValidationToolUsingSpark\\config\\driverConfig.ini')
print(configs)
print(configs.sections())
a = configs['sqlServer']['url']
print(a)



# from pyspark.sql.session import SparkSession
# spark = SparkSession \
#         .builder \
#         .appName("QA Validation Report Generation") \
#         .config("spark.driver.extraClassPath", "C:\\Work\\DataValidationToolUsingSpark\\lib\\*") \
#         .config("spark.driver.memory",'3g') \
#         .getOrCreate()
        
        
# url = 'jdbc:mysql://localhost:3306/warehouse?user=root&password=mysql@123'
# table = 'deposits'
# driver = 'com.mysql.cj.jdbc.Driver'
# df = spark.read.format("jdbc") \
#         .option("url", url) \
#         .option("dbtable", table) \
#         .option("driver", driver).load()
        
# print(df.show())