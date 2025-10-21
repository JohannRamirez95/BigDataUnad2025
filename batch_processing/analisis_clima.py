from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear la sesión de Spark
spark = SparkSession.builder.appName("ClimaFacatativa").getOrCreate()

# Leer todo el CSV como texto
raw = spark.read.text("/home/vboxuser/proyecto_streaming/clima_facatativa.csv")

# Mostrar las primeras líneas para confirmar
print("\n=== Primeras líneas del CSV ===")
raw.show(5, truncate=False)

# Extraer las líneas que realmente tienen datos (las que contienen comas y números)
datos = raw.filter(raw.value.like("2025%"))

# Dividir las columnas
df = datos.selectExpr(
    "split(value, ',')[0] as time",
    "split(value, ',')[1] as temperature",
    "split(value, ',')[2] as humidity",
    "split(value, ',')[3] as precipitation"
)

print("\n=== Datos procesados ===")
df.show(10, truncate=False)

# Convertir a tipos numéricos
df = df.withColumn("temperature", col("temperature").cast("float")) \
       .withColumn("humidity", col("humidity").cast("float")) \
       .withColumn("precipitation", col("precipitation").cast("float"))

print("\n=== Estadísticas descriptivas ===")
df.describe(["temperature", "humidity", "precipitation"]).show()

spark.stop()
print("\n✅ Análisis completado correctamente.")
