"""
conftest.py - Configuración compartida de pytest y fixtures
Este archivo se ejecuta automáticamente por pytest
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime, timedelta
import logging

# Configurar logging para tests
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def spark_session():
    """
    Fixture de Spark Session a nivel de sesión.
    Se crea una vez para todos los tests y se reutiliza.
    """
    logger.info("Inicializando Spark Session para tests...")
    
    spark = SparkSession.builder \
        .appName("WebLogAnalytics-TestSuite") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.ui.enabled", "false") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    logger.info(f"Spark Session iniciada - Versión: {spark.version}")
    
    yield spark
    
    logger.info("Cerrando Spark Session...")
    spark.stop()


@pytest.fixture(scope="function")
def spark(spark_session):
    """
    Fixture de Spark a nivel de función.
    Limpia el catálogo entre tests para evitar conflictos.
    """
    spark_session.catalog.clearCache()
    yield spark_session


@pytest.fixture
def sample_web_logs(spark):
    """
    Fixture que genera logs de muestra básicos para tests.
    """
    data = [
        ("log_1", "192.168.1.1", "2024-12-01 10:00:00", "/home", 200, 150, "Mozilla/5.0"),
        ("log_2", "192.168.1.2", "2024-12-01 10:05:00", "/api/products", 200, 250, "Chrome/120.0"),
        ("log_3", "192.168.1.1", "2024-12-01 10:10:00", "/api/users", 404, 100, "Mozilla/5.0"),
        ("log_4", "192.168.1.3", "2024-12-01 10:15:00", "/admin", 500, 5000, "Bot/1.0"),
        ("log_5", "192.168.1.2", "2024-12-01 10:20:00", "/checkout", 200, 300, "Safari/17.0"),
    ]
    
    schema = StructType([
        StructField("log_id", StringType(), False),
        StructField("ip_address", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("response_time_ms", IntegerType(), True),
        StructField("user_agent", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def large_web_logs(spark):
    """
    Fixture que genera un dataset más grande para tests de rendimiento.
    """
    import random
    from datetime import datetime, timedelta
    
    num_records = 1000
    ips = [f"192.168.{random.randint(1,255)}.{random.randint(1,255)}" for _ in range(50)]
    endpoints = ["/home", "/api/products", "/api/users", "/login", "/checkout"]
    status_codes = [200, 200, 200, 201, 404, 500]
    user_agents = ["Mozilla/5.0", "Chrome/120.0", "Safari/17.0", "Bot/1.0"]
    
    data = []
    base_time = datetime.now() - timedelta(days=7)
    
    for i in range(num_records):
        timestamp = base_time + timedelta(seconds=random.randint(0, 7*24*3600))
        data.append((
            f"log_{i}",
            random.choice(ips),
            timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            random.choice(endpoints),
            random.choice(status_codes),
            random.randint(100, 3000),
            random.choice(user_agents)
        ))
    
    schema = StructType([
        StructField("log_id", StringType(), False),
        StructField("ip_address", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("response_time_ms", IntegerType(), True),
        StructField("user_agent", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def temp_output_path(tmp_path):
    """
    Fixture que proporciona un directorio temporal para guardar outputs.
    Se limpia automáticamente después del test.
    """
    output_dir = tmp_path / "test_output"
    output_dir.mkdir()
    return str(output_dir)


# Hooks de pytest para personalizar el comportamiento

def pytest_configure(config):
    """
    Hook que se ejecuta al inicio de la sesión de pytest.
    """
    config.addinivalue_line(
        "markers", "slow: marca tests que tardan más de 1 segundo"
    )
    config.addinivalue_line(
        "markers", "integration: marca tests de integración end-to-end"
    )
    config.addinivalue_line(
        "markers", "unit: marca tests unitarios aislados"
    )
    config.addinivalue_line(
        "markers", "smoke: marca tests básicos de sanidad"
    )


def pytest_collection_modifyitems(config, items):
    """
    Hook que modifica los items de test recolectados.
    Añade marcadores automáticos basados en nombres.
    """
    for item in items:
        # Marcar tests lentos automáticamente
        if "large" in item.nodeid or "performance" in item.nodeid.lower():
            item.add_marker(pytest.mark.slow)
        
        # Marcar tests de integración
        if "integration" in item.nodeid.lower() or "full_pipeline" in item.nodeid.lower():
            item.add_marker(pytest.mark.integration)
        
        # Marcar tests unitarios
        if any(word in item.nodeid.lower() for word in ["unit", "test_data", "test_clean"]):
            item.add_marker(pytest.mark.unit)


def pytest_runtest_setup(item):
    """
    Hook que se ejecuta antes de cada test.
    Útil para logging o setup adicional.
    """
    logger.info(f"Ejecutando test: {item.nodeid}")


def pytest_runtest_teardown(item):
    """
    Hook que se ejecuta después de cada test.
    Útil para cleanup o logging.
    """
    logger.info(f"Test completado: {item.nodeid}")


# Fixtures adicionales para casos específicos

@pytest.fixture
def mock_config():
    """
    Fixture que proporciona configuración mock para tests.
    """
    return {
        "spark.sql.shuffle.partitions": "2",
        "app_name": "TestPipeline",
        "log_level": "ERROR"
    }


@pytest.fixture
def expected_schema():
    """
    Fixture que proporciona el schema esperado para validaciones.
    """
    return StructType([
        StructField("log_id", StringType(), False),
        StructField("ip_address", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("response_time_ms", IntegerType(), True),
        StructField("user_agent", StringType(), True)
    ])


# Helpers para assertions comunes

def assert_dataframe_equal(df1, df2, check_order=False):
    """
    Helper para comparar dos DataFrames de PySpark.
    """
    assert df1.schema == df2.schema, "Schemas diferentes"
    assert df1.count() == df2.count(), "Número de filas diferentes"
    
    if check_order:
        assert df1.collect() == df2.collect(), "Datos diferentes"
    else:
        assert set(df1.collect()) == set(df2.collect()), "Datos diferentes"


def assert_column_exists(df, column_name):
    """
    Helper para verificar que una columna existe.
    """
    assert column_name in df.columns, f"Columna '{column_name}' no existe"


def assert_no_nulls(df, column_name):
    """
    Helper para verificar que no hay nulos en una columna.
    """
    null_count = df.filter(df[column_name].isNull()).count()
    assert null_count == 0, f"Columna '{column_name}' tiene {null_count} valores nulos"


# Registrar helpers en pytest namespace para uso global
pytest.assert_dataframe_equal = assert_dataframe_equal
pytest.assert_column_exists = assert_column_exists
pytest.assert_no_nulls = assert_no_nulls