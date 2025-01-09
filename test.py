from pyspark.sql import SparkSession

# Initialiser une session Spark
spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()

df = spark.read.csv("/app/sales_data.csv", header=True, inferSchema=True)

df.show()