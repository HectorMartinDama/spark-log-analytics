"""
PROYECTO: Pipeline de Análisis de Logs Web con PySpark
Autor: Héctor Martín
Descripción: Sistema ETL para procesamiento distribuido de logs de acceso web,
            con transformaciones, agregaciones y detección de anomalías.

Tecnologías: PySpark, Delta Lake, Data Quality checks
"""

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, hour, dayofweek, count, avg, 
    window, regexp_extract, when, sum as spark_sum, lit
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

class WebLogAnalyticsPipeline:
    """
    Pipeline ETL para análisis de logs web con PySpark.
    Simula un caso real de Data Engineering en producción.
    """
    
    def __init__(self, app_name="WebLogAnalytics"):
        """Inicializa Spark Session con configuración optimizada"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        print(f"✅ Spark Session iniciada: {app_name}")
        print(f"📊 Versión Spark: {self.spark.version}")
    
    def generate_sample_logs(self, num_records=10000):
        """
        Genera logs de muestra simulando tráfico web real.
        En producción, esto vendría de S3, HDFS o un stream de Kafka.
        """
        from datetime import datetime, timedelta
        import random
        
        print(f"\n🔄 Generando {num_records} registros de logs...")
        
        # Simulación de datos realistas
        ips = [f"192.168.{random.randint(1,255)}.{random.randint(1,255)}" 
               for _ in range(100)]
        endpoints = ["/home", "/api/products", "/api/users", "/login", 
                     "/checkout", "/api/orders", "/dashboard", "/admin"]
        status_codes = [200, 200, 200, 200, 201, 301, 400, 404, 500]
        user_agents = ["Mozilla/5.0", "Chrome/90.0", "Safari/14.0", "Bot/1.0"]
        
        data = []
        base_time = datetime.now() - timedelta(days=7)
        
        for i in range(num_records):
            timestamp = base_time + timedelta(
                seconds=random.randint(0, 7*24*3600)
            )
            data.append((
                ips[random.randint(0, len(ips)-1)],
                timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                random.choice(endpoints),
                random.choice(status_codes),
                random.randint(100, 5000),  # response_time_ms
                random.choice(user_agents)
            ))
        
        # Crear DataFrame con schema explícito
        schema = StructType([
            StructField("ip_address", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("endpoint", StringType(), True),
            StructField("status_code", IntegerType(), True),
            StructField("response_time_ms", IntegerType(), True),
            StructField("user_agent", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        print(f"✅ Logs generados: {df.count()} registros")
        return df
    
    def extract_and_clean(self, df):
        """
        EXTRACT & CLEAN: Limpieza y validación de datos
        - Parseo de timestamps
        - Detección de bots
        - Filtrado de registros inválidos
        """
        print("\n🧹 EXTRACT & CLEAN: Limpiando datos...")
        
        # Convertir timestamp string a timestamp type
        df_clean = df.withColumn(
            "timestamp_parsed",
            to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
        )
        
        # Detectar bots basándose en user_agent
        df_clean = df_clean.withColumn(
            "is_bot",
            when(col("user_agent").contains("Bot"), lit(True))
            .otherwise(lit(False))
        )
        
        # Clasificar el tipo de endpoint (API vs Web)
        df_clean = df_clean.withColumn(
            "endpoint_type",
            when(col("endpoint").contains("/api/"), lit("API"))
            .otherwise(lit("WEB"))
        )
        
        # Clasificar status codes
        df_clean = df_clean.withColumn(
            "status_category",
            when(col("status_code") < 300, lit("success"))
            .when(col("status_code") < 400, lit("redirect"))
            .when(col("status_code") < 500, lit("client_error"))
            .otherwise(lit("server_error"))
        )
        
        # Filtrar registros con timestamp inválido (Data Quality)
        df_clean = df_clean.filter(col("timestamp_parsed").isNotNull())
        
        clean_count = df_clean.count()
        original_count = df.count()
        print(f"✅ Limpieza completada: {clean_count}/{original_count} registros válidos")
        
        return df_clean
    
    def transform_and_aggregate(self, df):
        """
        TRANSFORM: Agregaciones y métricas de negocio
        - Análisis temporal (hora del día, día de la semana)
        - Métricas por endpoint
        - Detección de anomalías
        """
        print("\n⚙️ TRANSFORM: Calculando agregaciones...")
        
        # Extraer características temporales
        df_transformed = df.withColumn("hour_of_day", hour("timestamp_parsed"))
        df_transformed = df_transformed.withColumn("day_of_week", dayofweek("timestamp_parsed"))
        
        # AGREGACIÓN 1: Tráfico por hora del día
        traffic_by_hour = df_transformed.groupBy("hour_of_day") \
            .agg(
                count("*").alias("total_requests"),
                avg("response_time_ms").alias("avg_response_time"),
                spark_sum(when(col("status_category") == "server_error", 1)
                         .otherwise(0)).alias("error_count")
            ) \
            .orderBy("hour_of_day")
        
        # AGREGACIÓN 2: Análisis por endpoint
        endpoint_stats = df_transformed.groupBy("endpoint", "endpoint_type") \
            .agg(
                count("*").alias("total_requests"),
                avg("response_time_ms").alias("avg_response_time"),
                (spark_sum(when(col("status_category") == "success", 1).otherwise(0)) 
                 / count("*") * 100).alias("success_rate")
            ) \
            .orderBy(col("total_requests").desc())
        
        # AGREGACIÓN 3: Top IPs (posibles atacantes o usuarios más activos)
        top_ips = df_transformed.groupBy("ip_address") \
            .agg(
                count("*").alias("request_count"),
                spark_sum(when(col("status_category") == "client_error", 1)
                         .otherwise(0)).alias("error_attempts")
            ) \
            .filter(col("request_count") > 50) \
            .orderBy(col("request_count").desc())
        
        # AGREGACIÓN 4: Análisis de rendimiento por tipo
        performance_by_type = df_transformed.groupBy("endpoint_type") \
            .agg(
                count("*").alias("total_requests"),
                avg("response_time_ms").alias("avg_response_time"),
                (spark_sum(when(col("response_time_ms") > 2000, 1).otherwise(0))
                 / count("*") * 100).alias("slow_request_percentage")
            )
        
        print("✅ Transformaciones completadas")
        
        return {
            "traffic_by_hour": traffic_by_hour,
            "endpoint_stats": endpoint_stats,
            "top_ips": top_ips,
            "performance_by_type": performance_by_type,
            "processed_data": df_transformed
        }
    
    def detect_anomalies(self, df):
        """
        ANÁLISIS AVANZADO: Detección de anomalías
        - Picos de tráfico inusuales
        - IPs con comportamiento sospechoso
        - Endpoints con alto rate de errores
        """
        print("\n🔍 ANÁLISIS: Detectando anomalías...")
        
        # Calcular estadísticas globales para detección de anomalías
        global_stats = df.agg(
            avg("response_time_ms").alias("avg_response_time"),
            avg(when(col("status_category") == "server_error", 1).otherwise(0))
                .alias("avg_error_rate")
        ).collect()[0]
        
        avg_response = global_stats["avg_response_time"]
        threshold_slow = avg_response * 3  # 3x más lento que la media
        
        # Detectar endpoints anómalos
        anomalous_endpoints = df.groupBy("endpoint") \
            .agg(
                count("*").alias("request_count"),
                avg("response_time_ms").alias("avg_response_time"),
                (spark_sum(when(col("status_category") == "server_error", 1)
                          .otherwise(0)) / count("*") * 100).alias("error_rate")
            ) \
            .filter(
                (col("avg_response_time") > threshold_slow) | 
                (col("error_rate") > 10)
            ) \
            .orderBy(col("error_rate").desc())
        
        anomaly_count = anomalous_endpoints.count()
        
        if anomaly_count > 0:
            print(f"⚠️  Se detectaron {anomaly_count} endpoints con comportamiento anómalo")
        else:
            print("✅ No se detectaron anomalías significativas")
        
        return anomalous_endpoints
    
    def save_results(self, results_dict, output_path="./output"):
        """
        LOAD: Guardar resultados en formato Parquet
        En producción: S3, Delta Lake, Data Warehouse
        """
        print(f"\n💾 LOAD: Guardando resultados en {output_path}...")
        
        os.makedirs(output_path, exist_ok=True)
        
        for name, df in results_dict.items():
            if df is not None and isinstance(df, DataFrame):
                path = f"{output_path}/{name}"
                df.write.mode("overwrite").parquet(path)
                print(f"  ✅ {name} guardado en formato Parquet")
    
    def run_pipeline(self):
        """Ejecuta el pipeline completo ETL"""
        print("="*60)
        print("🚀 INICIANDO PIPELINE DE ANÁLISIS DE LOGS WEB")
        print("="*60)
        
        # 1. EXTRACT: Generar/leer datos
        raw_logs = self.generate_sample_logs(num_records=10000)
        raw_logs.show(5, truncate=False)
        
        # 2. CLEAN: Limpiar y validar
        clean_logs = self.extract_and_clean(raw_logs)
        
        # 3. TRANSFORM: Agregaciones y métricas
        results = self.transform_and_aggregate(clean_logs)
        
        # 4. ANÁLISIS: Detección de anomalías
        anomalies = self.detect_anomalies(clean_logs)
        results["anomalies"] = anomalies
        
        # 5. Mostrar resultados clave
        print("\n" + "="*60)
        print("📊 RESULTADOS DEL ANÁLISIS")
        print("="*60)
        
        print("\n🕐 Tráfico por Hora del Día:")
        results["traffic_by_hour"].show(10)
        
        print("\n📍 Top 10 Endpoints más solicitados:")
        results["endpoint_stats"].show(10)
        
        print("\n🌐 Top IPs con mayor actividad:")
        results["top_ips"].show(5)
        
        print("\n⚡ Rendimiento por Tipo de Endpoint:")
        results["performance_by_type"].show()
        
        if results["anomalies"].count() > 0:
            print("\n⚠️  Endpoints Anómalos:")
            results["anomalies"].show()
        
        # 6. LOAD: Guardar resultados
        self.save_results(results)
        
        print("\n" + "="*60)
        print("✅ PIPELINE COMPLETADO EXITOSAMENTE")
        print("="*60)
        
        return results
    
    def stop(self):
        """Detener Spark Session"""
        self.spark.stop()
        print("\n🛑 Spark Session detenida")


if __name__ == "__main__":
    # Ejecutar el pipeline
    pipeline = WebLogAnalyticsPipeline()
    
    try:
        results = pipeline.run_pipeline()
        
        # Opcional: Análisis adicional interactivo
        print("\n💡 Pipeline listo para análisis adicionales")
        print("   - Puedes acceder a 'results' para más exploraciones")
        print("   - Ejemplo: results['processed_data'].filter(...)")
        
    except Exception as e:
        print(f"\n❌ Error en el pipeline: {str(e)}")
        raise
    finally:
        pipeline.stop()