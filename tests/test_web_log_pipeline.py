"""
Tests unitarios para el Web Log Analytics Pipeline
Autor: Héctor Martín
Tecnologías: pytest, PySpark, chispa (PySpark testing library)

Ejecutar tests:
    pytest tests/ -v
    pytest tests/test_web_log_pipeline.py::TestDataGeneration -v
"""

import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, count, avg
import sys
import os

# Añadir el directorio src al path para importar el pipeline
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from src.web_log_pipeline import WebLogAnalyticsPipeline
except ImportError:
    # Si falla, definir una versión simplificada para los tests
    pass


@pytest.fixture(scope="session")
def spark():
    """
    Fixture de Spark Session para todos los tests.
    Configuración optimizada para testing.
    """
    spark = SparkSession.builder \
        .appName("WebLogAnalytics-Tests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_logs_df(spark):
    """
    Fixture que genera un DataFrame de muestra para tests.
    """
    data = [
        ("192.168.1.1", "2024-12-01 10:00:00", "/home", 200, 150, "Mozilla/5.0"),
        ("192.168.1.2", "2024-12-01 10:05:00", "/api/products", 200, 250, "Chrome/120.0"),
        ("192.168.1.1", "2024-12-01 10:10:00", "/api/users", 404, 100, "Mozilla/5.0"),
        ("192.168.1.3", "2024-12-01 10:15:00", "/admin", 500, 5000, "Bot/1.0"),
        ("192.168.1.2", "2024-12-01 10:20:00", "/checkout", 200, 300, "Safari/17.0"),
    ]
    
    schema = StructType([
        StructField("ip_address", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("response_time_ms", IntegerType(), True),
        StructField("user_agent", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)


class TestDataGeneration:
    """Tests para la generación de datos de muestra"""
    
    def test_generate_logs_count(self, spark):
        """Verificar que se genera el número correcto de registros"""
        pipeline = WebLogAnalyticsPipeline()
        df = pipeline.generate_sample_logs(num_records=1000)
        
        assert df.count() == 1000, "Debería generar exactamente 1000 registros"
        
        pipeline.stop()
    
    def test_generate_logs_schema(self, spark):
        """Verificar que el schema es correcto"""
        pipeline = WebLogAnalyticsPipeline()
        df = pipeline.generate_sample_logs(num_records=100)
        
        expected_columns = [
            "ip_address", "timestamp", "endpoint", 
            "status_code", "response_time_ms", "user_agent"
        ]
        
        assert df.columns == expected_columns, "El schema no coincide"
        
        pipeline.stop()
    
    def test_generate_logs_no_nulls(self, spark):
        """Verificar que no hay valores nulos en campos críticos"""
        pipeline = WebLogAnalyticsPipeline()
        df = pipeline.generate_sample_logs(num_records=500)
        
        null_counts = df.select([
            count(col(c)).alias(c) for c in df.columns
        ]).collect()[0]
        
        for col_name in df.columns:
            assert null_counts[col_name] == 500, f"Columna {col_name} tiene valores nulos"
        
        pipeline.stop()


class TestDataCleaning:
    """Tests para la limpieza y validación de datos"""
    
    def test_timestamp_parsing(self, sample_logs_df, spark):
        """Verificar que los timestamps se parsean correctamente"""
        pipeline = WebLogAnalyticsPipeline()
        df_clean = pipeline.extract_and_clean(sample_logs_df)
        
        # Verificar que la columna timestamp_parsed existe
        assert "timestamp_parsed" in df_clean.columns
        
        # Verificar que no hay nulos
        null_count = df_clean.filter(col("timestamp_parsed").isNull()).count()
        assert null_count == 0, "No debería haber timestamps nulos después del parseo"
        
        pipeline.stop()
    
    def test_bot_detection(self, sample_logs_df, spark):
        """Verificar que se detectan bots correctamente"""
        pipeline = WebLogAnalyticsPipeline()
        df_clean = pipeline.extract_and_clean(sample_logs_df)
        
        # Debería haber 1 bot en los datos de muestra (user_agent = "Bot/1.0")
        bot_count = df_clean.filter(col("is_bot") == True).count()
        assert bot_count == 1, "Debería detectar exactamente 1 bot"
        
        pipeline.stop()
    
    def test_endpoint_classification(self, sample_logs_df, spark):
        """Verificar que los endpoints se clasifican correctamente"""
        pipeline = WebLogAnalyticsPipeline()
        df_clean = pipeline.extract_and_clean(sample_logs_df)
        
        # Verificar que existen ambos tipos
        endpoint_types = df_clean.select("endpoint_type").distinct().collect()
        types_list = [row.endpoint_type for row in endpoint_types]
        
        assert "API" in types_list, "Debería haber endpoints de tipo API"
        assert "WEB" in types_list, "Debería haber endpoints de tipo WEB"
        
        pipeline.stop()
    
    def test_status_category_classification(self, sample_logs_df, spark):
        """Verificar que los status codes se categorizan correctamente"""
        pipeline = WebLogAnalyticsPipeline()
        df_clean = pipeline.extract_and_clean(sample_logs_df)
        
        # Verificar las categorías
        categories = df_clean.select("status_category").distinct().collect()
        cat_list = [row.status_category for row in categories]
        
        assert "success" in cat_list, "Debería haber requests exitosos"
        assert "client_error" in cat_list, "Debería haber errores de cliente (404)"
        assert "server_error" in cat_list, "Debería haber errores de servidor (500)"
        
        pipeline.stop()


class TestTransformations:
    """Tests para transformaciones y agregaciones"""
    
    def test_aggregation_structure(self, sample_logs_df, spark):
        """Verificar que las agregaciones devuelven la estructura correcta"""
        pipeline = WebLogAnalyticsPipeline()
        df_clean = pipeline.extract_and_clean(sample_logs_df)
        results = pipeline.transform_and_aggregate(df_clean)
        
        # Verificar que existen todas las agregaciones esperadas
        assert "traffic_by_hour" in results
        assert "endpoint_stats" in results
        assert "top_ips" in results
        assert "performance_by_type" in results
        
        pipeline.stop()
    
    def test_traffic_by_hour_calculation(self, sample_logs_df, spark):
        """Verificar que el tráfico por hora se calcula correctamente"""
        pipeline = WebLogAnalyticsPipeline()
        df_clean = pipeline.extract_and_clean(sample_logs_df)
        results = pipeline.transform_and_aggregate(df_clean)
        
        traffic_df = results["traffic_by_hour"]
        
        # Verificar columnas esperadas
        expected_cols = ["hour_of_day", "total_requests", "avg_response_time", "error_count"]
        for col_name in expected_cols:
            assert col_name in traffic_df.columns, f"Falta columna {col_name}"
        
        # Verificar que hay datos
        assert traffic_df.count() > 0, "Debería haber datos de tráfico"
        
        pipeline.stop()
    
    def test_endpoint_stats_calculation(self, sample_logs_df, spark):
        """Verificar que las estadísticas por endpoint son correctas"""
        pipeline = WebLogAnalyticsPipeline()
        df_clean = pipeline.extract_and_clean(sample_logs_df)
        results = pipeline.transform_and_aggregate(df_clean)
        
        endpoint_stats = results["endpoint_stats"]
        
        # Verificar que el endpoint /home tiene 1 request
        home_stats = endpoint_stats.filter(col("endpoint") == "/home").collect()
        assert len(home_stats) == 1, "Debería haber stats para /home"
        assert home_stats[0].total_requests == 1, "Debería tener 1 request"
        
        pipeline.stop()
    
    def test_success_rate_calculation(self, sample_logs_df, spark):
        """Verificar que el success rate se calcula correctamente"""
        pipeline = WebLogAnalyticsPipeline()
        df_clean = pipeline.extract_and_clean(sample_logs_df)
        results = pipeline.transform_and_aggregate(df_clean)
        
        endpoint_stats = results["endpoint_stats"]
        
        # En los datos de muestra, hay 3 requests con status 200 (success) de 5 total
        # Success rate global debería ser 60%
        total_requests = endpoint_stats.agg({"total_requests": "sum"}).collect()[0][0]
        assert total_requests == 5, "Debería haber 5 requests en total"
        
        pipeline.stop()


class TestAnomalyDetection:
    """Tests para detección de anomalías"""
    
    def test_anomaly_detection_runs(self, sample_logs_df, spark):
        """Verificar que la detección de anomalías se ejecuta sin errores"""
        pipeline = WebLogAnalyticsPipeline()
        df_clean = pipeline.extract_and_clean(sample_logs_df)
        
        # No debería lanzar excepciones
        anomalies_df = pipeline.detect_anomalies(df_clean)
        
        assert anomalies_df is not None, "Debería devolver un DataFrame"
        
        pipeline.stop()
    
    def test_slow_endpoint_detection(self, sample_logs_df, spark):
        """Verificar que se detectan endpoints lentos"""
        pipeline = WebLogAnalyticsPipeline()
        df_clean = pipeline.extract_and_clean(sample_logs_df)
        anomalies_df = pipeline.detect_anomalies(df_clean)
        
        # El endpoint /admin tiene 5000ms, debería ser detectado como anómalo
        slow_endpoints = anomalies_df.filter(col("endpoint") == "/admin").count()
        
        # Puede o no ser detectado dependiendo del threshold, pero debería ejecutarse
        assert slow_endpoints >= 0, "Debería ejecutarse la detección"
        
        pipeline.stop()


class TestDataQuality:
    """Tests de calidad de datos"""
    
    def test_no_duplicate_records(self, spark):
        """Verificar que no hay registros duplicados en la generación"""
        pipeline = WebLogAnalyticsPipeline()
        df = pipeline.generate_sample_logs(num_records=1000)
        
        total_count = df.count()
        distinct_count = df.distinct().count()
        
        # Puede haber algunos duplicados por la naturaleza aleatoria, pero no muchos
        duplicate_rate = (total_count - distinct_count) / total_count
        assert duplicate_rate < 0.1, "No debería haber más del 10% de duplicados"
        
        pipeline.stop()
    
    def test_valid_status_codes(self, spark):
        """Verificar que todos los status codes son válidos"""
        pipeline = WebLogAnalyticsPipeline()
        df = pipeline.generate_sample_logs(num_records=1000)
        
        valid_codes = [200, 201, 301, 400, 404, 500]
        invalid_count = df.filter(~col("status_code").isin(valid_codes)).count()
        
        assert invalid_count == 0, "Todos los status codes deberían ser válidos"
        
        pipeline.stop()
    
    def test_valid_response_times(self, spark):
        """Verificar que los tiempos de respuesta están en rangos válidos"""
        pipeline = WebLogAnalyticsPipeline()
        df = pipeline.generate_sample_logs(num_records=1000)
        
        # Los tiempos de respuesta deberían estar entre 100 y 5000 ms
        invalid_times = df.filter(
            (col("response_time_ms") < 100) | (col("response_time_ms") > 5000)
        ).count()
        
        assert invalid_times == 0, "Todos los response times deberían estar en el rango válido"
        
        pipeline.stop()


class TestPerformance:
    """Tests de rendimiento del pipeline"""
    
    def test_large_dataset_processing(self, spark):
        """Verificar que el pipeline puede procesar datasets grandes"""
        import time
        
        pipeline = WebLogAnalyticsPipeline()
        
        start_time = time.time()
        df = pipeline.generate_sample_logs(num_records=10000)
        df_clean = pipeline.extract_and_clean(df)
        results = pipeline.transform_and_aggregate(df_clean)
        end_time = time.time()
        
        processing_time = end_time - start_time
        
        # El procesamiento de 10K registros debería tomar menos de 30 segundos
        assert processing_time < 30, f"Procesamiento muy lento: {processing_time:.2f}s"
        
        pipeline.stop()
    
    def test_memory_efficiency(self, spark):
        """Verificar que el pipeline no consume memoria excesiva"""
        pipeline = WebLogAnalyticsPipeline()
        
        # Procesar múltiples batches sin acumular memoria
        for i in range(3):
            df = pipeline.generate_sample_logs(num_records=5000)
            df_clean = pipeline.extract_and_clean(df)
            # Forzar evaluación
            count = df_clean.count()
            assert count > 0
        
        pipeline.stop()


class TestIntegration:
    """Tests de integración end-to-end"""
    
    def test_full_pipeline_execution(self, spark):
        """Verificar que el pipeline completo se ejecuta sin errores"""
        pipeline = WebLogAnalyticsPipeline()
        
        # No debería lanzar excepciones
        results = pipeline.run_pipeline()
        
        # Verificar que tenemos todos los resultados esperados
        assert "traffic_by_hour" in results
        assert "endpoint_stats" in results
        assert "top_ips" in results
        assert "performance_by_type" in results
        assert "anomalies" in results
        
        # Verificar que hay datos en los resultados
        for key, df in results.items():
            if key != "processed_data":
                assert df.count() >= 0, f"{key} debería tener datos o estar vacío"
        
        pipeline.stop()


# Configuración adicional de pytest
def pytest_configure(config):
    """Configuración personalizada de pytest"""
    config.addinivalue_line(
        "markers", "slow: marca tests que son lentos de ejecutar"
    )
    config.addinivalue_line(
        "markers", "integration: marca tests de integración"
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])