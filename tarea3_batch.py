from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, min, max, count, year, desc

spark = SparkSession.builder \
    .appName("Tarea3_Batch_TRM_Ginna") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Ruta del dataset almacenado en HDFS
ruta = "hdfs://localhost:9000/Tarea3/rows.csv"

# Cargar archivo CSV
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(ruta)

print("\n==========================================")
print("1. ESQUEMA DEL DATASET")
print("==========================================")
df.printSchema()

print("\n==========================================")
print("2. PRIMEROS REGISTROS")
print("==========================================")
df.show(20, truncate=False)

print("\n==========================================")
print("3. CANTIDAD DE REGISTROS")
print("==========================================")
print("Total de registros:", df.count())

# Limpieza
df_limpio = df.dropDuplicates() \
    .dropna(subset=["VALOR", "VIGENCIADESDE"])

df_limpio = df_limpio.withColumn(
    "VALOR",
    col("VALOR").cast("double")
)

df_limpio = df_limpio.withColumn(
    "FECHA",
    to_date(col("VIGENCIADESDE"), "dd/MM/yyyy")
)

df_limpio = df_limpio.filter(
    col("VALOR").isNotNull() &
    col("FECHA").isNotNull()
)

print("\n==========================================")
print("4. REGISTROS DESPUÉS DE LA LIMPIEZA")
print("==========================================")
print("Total de registros limpios:", df_limpio.count())

print("\n==========================================")
print("5. ESTADÍSTICAS DESCRIPTIVAS")
print("==========================================")
df_limpio.select("VALOR").summary().show()

print("\n==========================================")
print("6. INDICADORES PRINCIPALES")
print("==========================================")
df_limpio.select(
    count("VALOR").alias("Cantidad"),
    avg("VALOR").alias("Promedio_TRM"),
    min("VALOR").alias("TRM_Minima"),
    max("VALOR").alias("TRM_Maxima")
).show(truncate=False)

print("\n==========================================")
print("7. TRM SUPERIOR A 5000 PESOS")
print("==========================================")
df_limpio \
    .filter(col("VALOR") > 5000) \
    .select("VALOR", "VIGENCIADESDE", "VIGENCIAHASTA") \
    .orderBy(desc("VALOR")) \
    .show(20, truncate=False)

print("\n==========================================")
print("8. LOS 10 VALORES MÁS ALTOS")
print("==========================================")
df_limpio \
    .select("FECHA", "VALOR") \
    .orderBy(desc("VALOR")) \
    .show(10, truncate=False)

print("\n==========================================")
print("9. PROMEDIO ANUAL DE LA TRM")
print("==========================================")

promedio_anual = df_limpio \
    .withColumn("ANIO", year(col("FECHA"))) \
    .groupBy("ANIO") \
    .agg(avg("VALOR").alias("PROMEDIO_TRM")) \
    .orderBy("ANIO")

promedio_anual.show(100, truncate=False)

print("\n==========================================")
print("ANÁLISIS FINAL")
print("==========================================")

print("El procesamiento batch permitió cargar, limpiar, transformar")
print("y analizar el comportamiento histórico de la TRM mediante Apache Spark.")

print("Se calcularon estadísticas descriptivas, valores máximos y mínimos,")
print("registros superiores a 5000 pesos y el promedio anual de la TRM.")

spark.stop()
