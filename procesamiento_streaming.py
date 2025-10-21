from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

spark = SparkSession.builder \
    .appName("KafkaStreamingTemperaturas") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "temperaturas") \
    .load()

df = df.selectExpr("CAST(value AS STRING)")

df_split = df.withColumn("Ciudad", split(col("value"), ",").getItem(0)) \
             .withColumn("Temperatura", split(col("value"), ",").getItem(1).cast("float")) \
             .select("Ciudad", "Temperatura")

consulta = df_split.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

consulta.awaitTermination()
