![Tests](https://github.com/HectorMartinDama/spark-log-analytics/workflows/Tests%20and%20CI/badge.svg)
![Coverage](https://img.shields.io/badge/coverage-85%25-green)



# 🚀 Web Log Analytics Pipeline - PySpark

> **Pipeline ETL distribuido para análisis de logs web con Apache Spark**

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-4.0.1-orange)](https://spark.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

## 📋 Descripción del Proyecto

Sistema de análisis de logs web implementado con **PySpark** que simula un caso real de **Data Engineering en producción**. El proyecto implementa un pipeline ETL completo con:

- ✅ **Procesamiento distribuido** de grandes volúmenes de datos
- ✅ **Transformaciones complejas** con agregaciones y window functions
- ✅ **Data Quality checks** y validación de datos
- ✅ **Detección de anomalías** en tiempo de procesamiento
- ✅ **Optimización de queries** con particionamiento inteligente
- ✅ **Formato Parquet** para almacenamiento en columna eficiente

### 🎯 Casos de Uso

1. **Análisis de tráfico web**: Identificar patrones horarios y días pico
2. **Monitorización de rendimiento**: Detectar endpoints lentos o con errores
3. **Seguridad**: Identificar IPs sospechosas con alto volumen de requests
4. **Business Intelligence**: Métricas de negocio sobre uso de APIs vs Web

---

## 🏗️ Arquitectura del Pipeline

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐      ┌──────────┐
│   EXTRACT   │ ───> │    CLEAN     │ ───> │  TRANSFORM  │ ───> │   LOAD   │
│             │      │              │      │             │      │          │
│ • Raw Logs  │      │ • Validation │      │ • Aggreg.   │      │ • Parquet│
│ • S3/HDFS   │      │ • Parsing    │      │ • Analytics │      │ • Delta  │
│ • Streaming │      │ • Quality    │      │ • Anomalies │      │ • S3     │
└─────────────┘      └──────────────┘      └─────────────┘      └──────────┘
```

### Componentes Clave

1. **Extract**: Generación/Lectura de logs (simulación de S3/Kafka)
2. **Clean**:
   - Parseo de timestamps
   - Detección de bots
   - Clasificación de endpoints (API/WEB)
   - Filtrado de registros inválidos
3. **Transform**:
   - Agregaciones temporales (hora, día)
   - Métricas por endpoint
   - Análisis de rendimiento
   - Top IPs sospechosas
4. **Anomalies Detection**:
   - Endpoints con alto error rate
   - Response times anómalos (>3σ)
5. **Load**: Persistencia en formato Parquet optimizado

---

## 🚀 Quick Start

### Prerequisitos

```bash
# Python 3.8+
python --version

# Java 8 o 11 (requerido por Spark)
java -version
```

### Instalación

```bash
# 1. Clonar el repositorio
git clone https://github.com/HectorMartinDama/spark-log-analytics.git
cd spark-log-analytics

# 2. Crear entorno virtual
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate

# 3. Instalar dependencias
pip install pyspark==4.0.1
pip install pandas  # opcional, para análisis adicional

# 4. Ejecutar el pipeline
python web_log_pipeline.py
```
---

## 📊 Resultados del Análisis

### 1. Tráfico por Hora del Día

```
+------------+--------------+------------------+-----------+
|hour_of_day |total_requests|avg_response_time |error_count|
+------------+--------------+------------------+-----------+
|0           |342           |1523.4            |12         |
|1           |298           |1487.2            |8          |
...
```

### 2. Top Endpoints

```
+------------------+-------------+--------------+------------------+------------+
|endpoint          |endpoint_type|total_requests|avg_response_time |success_rate|
+------------------+-------------+--------------+------------------+------------+
|/api/products     |API          |1523          |1234.5            |98.2        |
|/home             |WEB          |1456          |876.3             |99.1        |
...
```

### 3. Anomalías Detectadas

```
⚠️  Se detectaron 3 endpoints con comportamiento anómalo
+------------------+--------------+------------------+----------+
|endpoint          |request_count |avg_response_time |error_rate|
+------------------+--------------+------------------+----------+
|/admin            |234           |4532.1            |23.5      |
```

---

## 🔧 Configuración Avanzada

### Lectura desde S3 (Producción)

```python
# Configurar credenciales AWS
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "YOUR_KEY")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "YOUR_SECRET")

# Leer logs desde S3
df = spark.read.json("s3a://your-bucket/logs/year=2024/month=12/*")
```

### Integración con Databricks

```python
# En Databricks Notebook
from web_log_pipeline import WebLogAnalyticsPipeline

# Leer desde Delta Lake
df = spark.read.format("delta").load("/mnt/logs/web_logs")

# Ejecutar pipeline
pipeline = WebLogAnalyticsPipeline()
results = pipeline.transform_and_aggregate(df)

# Guardar en Delta
results["endpoint_stats"] \
    .write.format("delta") \
    .mode("overwrite") \
    .save("/mnt/analytics/endpoint_stats")
```

---

## 📈 Métricas de Rendimiento

| Métrica              | Valor                       |
| -------------------- | --------------------------- |
| Registros procesados | 10,000                      |
| Tiempo de ejecución  | ~15 segundos                |
| Particiones Spark    | 4 (optimizado)              |
| Formato salida       | Parquet (compresión snappy) |
| Reducción de tamaño  | ~70% vs CSV                 |

---

## 🧪 Testing

```bash
# Ejecutar con dataset de prueba
python web_log_pipeline.py --num-records 1000

# Modo debug
python web_log_pipeline.py --log-level DEBUG
```

---

## 🛠️ Tecnologías Utilizadas

- **Apache Spark**: Motor de procesamiento distribuido
- **PySpark**: API de Python para Spark
- **Parquet**: Formato columnar de almacenamiento
- **Delta Lake** (opcional): Gestión de data lakes con ACID
- **Pandas** (opcional): Análisis complementario

---

---

## 🎓 Conceptos de Data Engineering Demostrados

✅ **ETL Pipelines**: Extracción, Transformación y Carga de datos  
✅ **Distributed Computing**: Procesamiento paralelo con Spark  
✅ **Data Quality**: Validación y limpieza de datos  
✅ **Aggregations**: Group by, window functions, joins  
✅ **Optimization**: Partitioning, caching, broadcast joins  
✅ **Anomaly Detection**: Estadística descriptiva y umbrales

---

## 👤 Autor

**Héctor Martín**

- 💼 LinkedIn: [HectorMartinDama]
- 🐙 GitHub: [@HectorMartinDama]
- 📧 Email: hectormartindama@gmail.com

---

## 🙏 Agradecimientos

Proyecto creado como demostración de habilidades en **Data Engineering con PySpark** para procesos de selección en el sector tech.

---

**⭐ Si te resultó útil este proyecto, considera darle una estrella en GitHub**
