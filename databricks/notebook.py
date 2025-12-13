# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Web Log Analytics Pipeline - Databricks Edition
# MAGIC
# MAGIC **Autor**: Héctor Martín  
# MAGIC **Fecha**: Diciembre 2024  
# MAGIC **Objetivo**: Pipeline ETL distribuido para análisis de logs web
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 Arquitectura del Pipeline
# MAGIC
# MAGIC ```
# MAGIC Delta Lake (Bronze) → Spark Processing → Delta Lake (Silver) → Delta Lake (Gold)
# MAGIC      Raw Logs             Cleaning           Analytics          Business Metrics
# MAGIC ```
# MAGIC
# MAGIC ### Tecnologías Utilizadas
# MAGIC - **Spark 4.0** con Adaptive Query Execution
# MAGIC - **Delta Lake** para gestión ACID de datos
# MAGIC - **Databricks Runtime** optimizado
# MAGIC - **Unity Catalog** para data governance

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuración Inicial

# COMMAND ----------

# Imports necesarios
from pyspark.sql.functions import (
    col, to_timestamp, hour, dayofweek, count, avg, 
    sum as spark_sum, when, lit, current_timestamp, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime, timedelta
import random

print(f"📊 Versión Spark: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Creacion de Volumenes y Esquemas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS main;
# MAGIC CREATE SCHEMA IF NOT EXISTS main.bronze;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS main.bronze.web_logs;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS main.silver;
# MAGIC CREATE VOLUME IF NOT EXISTS main.silver.web_logs_cleaned;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS main.gold;
# MAGIC CREATE VOLUME IF NOT EXISTS main.gold.web_analytics;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 BRONZE LAYER: Ingestión de Datos Brutos

# COMMAND ----------

# Definir paths en Volumenes (alternativa actual a DBFS)
bronze_path = "/Volumes/main/bronze/web_logs"
silver_path = "/Volumes/main/silver/web_logs_cleaned"
gold_path = "/Volumes/main/gold/web_analytics"

# Generar datos de muestra
def generate_sample_logs(num_records=50000):
    """Genera logs de muestra simulando tráfico web real"""
    
    print(f"🔄 Generando {num_records} registros de logs...")
    
    # Datos realistas
    ips = [f"192.168.{random.randint(1,255)}.{random.randint(1,255)}" for _ in range(500)]
    endpoints = [
        "/home", "/api/products", "/api/users", "/login", "/checkout",
        "/api/orders", "/dashboard", "/admin", "/api/search", "/profile"
    ]
    status_codes = [200]*70 + [201]*10 + [301]*5 + [400]*8 + [404]*5 + [500]*2
    user_agents = ["Mozilla/5.0", "Chrome/120.0", "Safari/17.0", "Bot/1.0", "Mobile/iOS"]
    countries = ["ES", "US", "UK", "DE", "FR", "IT", "BR", "MX"]
    
    data = []
    base_time = datetime.now() - timedelta(days=30)  # Últimos 30 días
    
    for i in range(num_records):
        timestamp = base_time + timedelta(seconds=random.randint(0, 30*24*3600))
        data.append((
            f"log_{i}",
            ips[random.randint(0, len(ips)-1)],
            timestamp,
            random.choice(endpoints),
            random.choice(status_codes),
            random.randint(50, 5000),  # response_time_ms
            random.choice(user_agents),
            random.choice(countries),
            random.randint(100, 100000)  # bytes_sent
        ))
    
    # Schema explícito
    schema = StructType([
        StructField("log_id", StringType(), False),
        StructField("ip_address", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("endpoint", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("response_time_ms", IntegerType(), True),
        StructField("user_agent", StringType(), True),
        StructField("country", StringType(), True),
        StructField("bytes_sent", IntegerType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    print(f"✅ Logs generados: {df.count():,} registros")
    
    return df

# Generar datos
bronze_df = generate_sample_logs(50000)

# Guardar en Delta Lake (Bronze Layer)
bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("country") \
    .save(bronze_path)

print(f"✅ Datos guardados en Bronze Layer: {bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Exploración de Datos Brutos

# COMMAND ----------

# Leer desde Delta Lake
bronze_df = spark.read.format("delta").load(bronze_path)

# Estadísticas básicas
print("📊 ESTADÍSTICAS DEL DATASET BRONZE")
print("="*60)
print(f"Total registros: {bronze_df.count():,}")
print(f"Período: {bronze_df.select('timestamp').agg({'timestamp': 'min'}).collect()[0][0]} a {bronze_df.select('timestamp').agg({'timestamp': 'max'}).collect()[0][0]}")
print(f"Columnas: {len(bronze_df.columns)}")

display(bronze_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 SILVER LAYER: Limpieza y Enriquecimiento

# COMMAND ----------

# Limpieza y transformaciones
silver_df = bronze_df

# 1. Añadir timestamp de procesamiento
silver_df = silver_df.withColumn("processed_at", current_timestamp())

# 2. Extraer características temporales
silver_df = silver_df \
    .withColumn("hour_of_day", hour("timestamp")) \
    .withColumn("day_of_week", dayofweek("timestamp")) \
    .withColumn("date", col("timestamp").cast("date"))

# 3. Detectar bots
silver_df = silver_df.withColumn(
    "is_bot",
    when(col("user_agent").contains("Bot"), True).otherwise(False)
)

# 4. Clasificar tipo de endpoint
silver_df = silver_df.withColumn(
    "endpoint_type",
    when(col("endpoint").startswith("/api/"), "API")
    .when(col("endpoint").startswith("/admin"), "ADMIN")
    .otherwise("WEB")
)

# 5. Clasificar status codes
silver_df = silver_df.withColumn(
    "status_category",
    when(col("status_code") < 300, "SUCCESS")
    .when(col("status_code") < 400, "REDIRECT")
    .when(col("status_code") < 500, "CLIENT_ERROR")
    .otherwise("SERVER_ERROR")
)

# 6. Clasificar rendimiento
silver_df = silver_df.withColumn(
    "performance_class",
    when(col("response_time_ms") < 500, "FAST")
    .when(col("response_time_ms") < 2000, "NORMAL")
    .otherwise("SLOW")
)

# 7. Data Quality: Eliminar registros inválidos
silver_df = silver_df.filter(
    col("timestamp").isNotNull() & 
    col("ip_address").isNotNull() &
    col("endpoint").isNotNull()
)

# Guardar en Silver Layer
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("date", "country") \
    .save(silver_path)

print(f"✅ Datos limpiados guardados en Silver Layer: {silver_path}")
print(f"📊 Registros procesados: {silver_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Visualización de Datos Limpios

# COMMAND ----------

silver_df = spark.read.format("delta").load(silver_path)

# Distribución de status codes
display(
    silver_df.groupBy("status_category")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏆 GOLD LAYER: Agregaciones y Métricas de Negocio

# COMMAND ----------

# AGREGACIÓN 1: Tráfico por hora y tipo
traffic_by_hour = silver_df.groupBy("hour_of_day", "endpoint_type") \
    .agg(
        count("*").alias("total_requests"),
        avg("response_time_ms").alias("avg_response_time"),
        (spark_sum(when(col("status_category") == "SUCCESS", 1).otherwise(0)) / count("*") * 100).alias("success_rate"),
        spark_sum("bytes_sent").alias("total_bytes_sent")
    ) \
    .orderBy("hour_of_day", "endpoint_type")

# Guardar en Gold
traffic_by_hour.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/traffic_by_hour")

print("✅ Métrica 1: Tráfico por hora guardada")
display(traffic_by_hour)

# COMMAND ----------

# AGREGACIÓN 2: Performance por endpoint
endpoint_performance = silver_df.groupBy("endpoint", "endpoint_type") \
    .agg(
        count("*").alias("total_requests"),
        avg("response_time_ms").alias("avg_response_time"),
        (spark_sum(when(col("performance_class") == "SLOW", 1).otherwise(0)) / count("*") * 100).alias("slow_request_pct"),
        (spark_sum(when(col("status_category") == "SUCCESS", 1).otherwise(0)) / count("*") * 100).alias("success_rate")
    ) \
    .orderBy(col("total_requests").desc())

endpoint_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/endpoint_performance")

print("✅ Métrica 2: Performance por endpoint guardada")
display(endpoint_performance.limit(15))

# COMMAND ----------

# AGREGACIÓN 3: Análisis geográfico
geo_analysis = silver_df.groupBy("country") \
    .agg(
        count("*").alias("total_requests"),
        avg("response_time_ms").alias("avg_response_time"),
        spark_sum("bytes_sent").alias("total_bandwidth_bytes"),
        (spark_sum(when(col("is_bot"), 1).otherwise(0)) / count("*") * 100).alias("bot_percentage")
    ) \
    .withColumn("bandwidth_mb", col("total_bandwidth_bytes") / 1024 / 1024) \
    .orderBy(col("total_requests").desc())

geo_analysis.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/geo_analysis")

print("✅ Métrica 3: Análisis geográfico guardado")
display(geo_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Detección de Anomalías con SQL

# COMMAND ----------

/Volumes/main/bronze/web_logs/_delta_log/00000000000000000000.json# Crear vista temporal
silver_df.createOrReplaceTempView("web_logs")

# Query SQL para detectar anomalías
anomalies_sql = """
WITH endpoint_stats AS (
    SELECT 
        endpoint,
        endpoint_type,
        COUNT(*) as request_count,
        AVG(response_time_ms) as avg_response_time,
        PERCENTILE(response_time_ms, 0.95) as p95_response_time,
        SUM(CASE WHEN status_category = 'SERVER_ERROR' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as error_rate
    FROM web_logs
    GROUP BY endpoint, endpoint_type
),
global_stats AS (
    SELECT 
        AVG(avg_response_time) as global_avg_response,
        STDDEV(avg_response_time) as global_std_response
    FROM endpoint_stats
)
SELECT 
    e.endpoint,
    e.endpoint_type,
    e.request_count,
    ROUND(e.avg_response_time, 2) as avg_response_time,
    ROUND(e.p95_response_time, 2) as p95_response_time,
    ROUND(e.error_rate, 2) as error_rate,
    CASE 
        WHEN e.avg_response_time > g.global_avg_response + 2 * g.global_std_response THEN 'SLOW'
        WHEN e.error_rate > 5 THEN 'HIGH_ERROR'
        ELSE 'NORMAL'
    END as anomaly_type
FROM endpoint_stats e
CROSS JOIN global_stats g
WHERE e.avg_response_time > g.global_avg_response + 2 * g.global_std_response
   OR e.error_rate > 5
ORDER BY e.error_rate DESC, e.avg_response_time DESC
"""

anomalies_df = spark.sql(anomalies_sql)

anomalies_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/anomalies")

print("✅ Detección de anomalías completada")
display(anomalies_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Dashboard de Métricas Clave (KPIs)

# COMMAND ----------

# KPIs principales
kpis = silver_df.select(
    count("*").alias("total_requests"),
    avg("response_time_ms").alias("avg_response_time"),
    (spark_sum(when(col("status_category") == "SUCCESS", 1).otherwise(0)) / count("*") * 100).alias("overall_success_rate"),
    (spark_sum(when(col("is_bot"), 1).otherwise(0)) / count("*") * 100).alias("bot_traffic_pct"),
    spark_sum("bytes_sent").alias("total_bandwidth_bytes")
).withColumn("total_bandwidth_gb", col("total_bandwidth_bytes") / 1024 / 1024 / 1024)

print("📊 KPIs PRINCIPALES DEL SISTEMA")
print("="*60)
display(kpis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Top 10 IPs más Activas

# COMMAND ----------

top_ips = silver_df.groupBy("ip_address") \
    .agg(
        count("*").alias("request_count"),
        spark_sum(when(col("status_category").isin(["CLIENT_ERROR", "SERVER_ERROR"]), 1).otherwise(0)).alias("error_count"),
        avg("response_time_ms").alias("avg_response_time")
    ) \
    .filter(col("request_count") > 100) \
    .orderBy(col("request_count").desc()) \
    .limit(10)

display(top_ips)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Análisis de Tendencias Temporales

# COMMAND ----------

# Tendencia diaria
daily_trends = silver_df.groupBy("date") \
    .agg(
        count("*").alias("total_requests"),
        avg("response_time_ms").alias("avg_response_time"),
        (spark_sum(when(col("status_category") == "SUCCESS", 1).otherwise(0)) / count("*") * 100).alias("success_rate")
    ) \
    .orderBy("date")

display(daily_trends)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Resumen del Pipeline
# MAGIC
# MAGIC ### ✅ Capas Implementadas
# MAGIC
# MAGIC 1. **Bronze Layer**: Datos brutos particionados por país
# MAGIC 2. **Silver Layer**: Datos limpiados y enriquecidos con particionamiento por fecha
# MAGIC 3. **Gold Layer**: Agregaciones y métricas de negocio
# MAGIC
# MAGIC ### 🎯 Métricas Generadas
# MAGIC
# MAGIC - Tráfico por hora y tipo de endpoint
# MAGIC - Performance por endpoint
# MAGIC - Análisis geográfico
# MAGIC - Detección de anomalías
# MAGIC - KPIs principales del sistema
# MAGIC ---
# MAGIC
# MAGIC **Proyecto creado por**: Héctor Martín  
# MAGIC **Tecnologías**: PySpark 4.0, Delta Lake, Databricks Runtime  
# MAGIC **Fecha**: Diciembre 2024