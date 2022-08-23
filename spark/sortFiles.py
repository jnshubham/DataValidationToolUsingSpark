from pyspark.sql.session import SparkSession
spark = SparkSession \
        .builder \
        .appName("QA Validation Report Generation") \
        .config("spark.driver.memory",'60g') \
        .getOrCreate()
        
import pyspark.sql.functions as f
df = spark.createDataFrame([(1,'a',1.003,1,'a',1.003),(1,'a',1.004,1,'a',1.003)],['id','name','col','id2','name2','col2'])
#df.select(f.when(f.col('name').cast('int').isNotNull(),'True').otherwise('False')).show()
a = df.count()
with open(r'.\text.csv','w+') as t:
    t.write(str(a))
df = spark.createDataFrame([(1,'a',1.003,1,'a',1.003),(1,'a',1.004,1,'a',1.003)],['id','name','col','id2','name2','col2'])
#df.show()
#"C:\\Users\\shujain8\\OneDrive - Publicis Groupe\\Documents\\GitHub\\DataValidationToolUsingSpark\\sortFiles.py"