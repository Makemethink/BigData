# Necessary imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col

 Create Spark session object
spark = SparkSession.builder.master("local").appName("LogExtraction").getOrCreate()

# rdd = sc.textFile("path/to/your/textfile.txt")

df = spark.read.text('./../Source/sample.txt')  # default column name value

pattern = r'(\S+ \S+) (\S+) (\S+) (.*)'

df = df.withColumnRenamed('value', 'log')


# \S+ -> String
# ()  -> Group
# .*  -> All the other

df_extracted = df.withColumn('timestamp', regexp_extract( col('log'), pattern, 1)) \
                 .withColumn('type', regexp_extract(col('log'), pattern, 2)) \
                 .withColumn('source', regexp_extract(col('log'), pattern, 3)) \
                 .withColumn('message', regexp_extract(col('log'), pattern, 4)) \
                 .drop(col('log'))

# Save the results
df_extracted.write.csv("./Output/", header=True, mode="overwrite")

# Stopping the spark session
spark.stop()
