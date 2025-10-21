from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("ClimaFacatativaStreaming") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Esquema de los datos JSON que envía el productor
schema = StructType([
    StructField("hora", StringType()),
    StructField("temperatura", DoubleType()),
    StructField("humedad", DoubleType()),
    StructField("precipitacion", DoubleType())
])

# Leer flujo en tiempo real desde Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clima_facatativa") \
    .load()

# Parsear el valor JSON
parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Convertir hora a tipo timestamp
parsed = parsed.withColumn("hora", col("hora").cast(TimestampType()))

# Calcular promedio de temperatura y humedad cada minuto
promedios = parsed.groupBy(window(col("hora"), "1 minute")) \
    .agg(avg("temperatura").alias("temp_prom"), avg("humedad").alias("hum_prom"))

# Mostrar resultados en consola en tiempo real
query = promedios.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()
