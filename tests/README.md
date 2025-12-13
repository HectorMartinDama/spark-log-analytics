# 🧪 Suite de Tests - Web Log Analytics Pipeline

Esta carpeta contiene todos los tests automatizados del proyecto.

## 📂 Estructura

```
tests/
├── test_web_log_pipeline.py # Tests principales del pipeline
└── README.md               # Esta documentación
```

## 🚀 Ejecutar Tests

### Instalar dependencias de testing

```bash
pip install -r requirements-dev.txt
```

### Ejecutar todos los tests

```bash
# Opción 1: Con pytest directamente
pytest tests/ -v

# Opción 2: Con Makefile
make test

# Opción 3: Con cobertura de código
make test-coverage
```

### Ejecutar tests específicos

```bash
# Solo tests unitarios
pytest tests/ -v -m unit
# o
make test-unit

# Solo tests de integración
pytest tests/ -v -m integration
# o
make test-integration

# Tests rápidos (excluye lentos)
pytest tests/ -v -m "not slow"
# o
make test-fast

# Un test específico
pytest tests/test_web_log_pipeline.py::TestDataGeneration::test_generate_logs_count -v
```

### Ejecutar con opciones útiles

```bash
# Ver output completo (incluso prints)
pytest tests/ -v -s

# Detener en el primer fallo
pytest tests/ -v -x

# Ejecutar tests en paralelo (más rápido)
pytest tests/ -v -n auto

# Ejecutar solo tests que fallaron la última vez
pytest tests/ --lf
```

## 📊 Cobertura de Código

```bash
# Generar reporte de cobertura HTML
pytest tests/ --cov=src --cov-report=html

# Ver reporte en el navegador
open htmlcov/index.html  # En MacOS
xdg-open htmlcov/index.html  # En Linux
start htmlcov/index.html  # En Windows
```

## 🎯 Categorías de Tests

### Tests Unitarios (`-m unit`)

Prueban funciones individuales de forma aislada.

**Ejemplos:**

- `TestDataGeneration`: Generación de datos
- `TestDataCleaning`: Limpieza y validación
- `TestTransformations`: Transformaciones individuales

### Tests de Integración (`-m integration`)

Prueban el flujo completo del pipeline.

**Ejemplos:**

- `TestIntegration::test_full_pipeline_execution`

### Tests Lentos (`-m slow`)

Tests que requieren más tiempo de ejecución.

**Ejemplos:**

- `TestPerformance::test_large_dataset_processing`

## 📝 Escribir Nuevos Tests

### Template básico

```python
import pytest
from pyspark.sql.functions import col

def test_mi_funcionalidad(spark, sample_web_logs):
    """
    Test que verifica [descripción clara].
    """
    # Arrange (preparar)
    df = sample_web_logs

    # Act (ejecutar)
    result = df.filter(col("status_code") == 200)

    # Assert (verificar)
    assert result.count() == 3, "Debería haber 3 requests con status 200"
```

### Usar fixtures

```python
def test_con_fixture(spark, sample_web_logs):
    """
    Las fixtures se inyectan automáticamente.
    """
    assert sample_web_logs.count() == 5

def test_con_fixture_temporal(spark, temp_output_path):
    """
    temp_output_path se limpia automáticamente después del test.
    """
    output_path = f"{temp_output_path}/test_data"
    # ... guardar datos en output_path
```

### Marcar tests

```python
@pytest.mark.slow
def test_operacion_lenta():
    """Este test se puede excluir con -m 'not slow'"""
    pass

@pytest.mark.integration
def test_integracion_completa():
    """Este test se ejecuta solo con -m integration"""
    pass
```

## 🔧 Fixtures Disponibles

### Fixtures de Spark

- **`spark_session`** (scope: session): Spark Session compartida para toda la suite
- **`spark`** (scope: function): Spark Session limpia para cada test

### Fixtures de Datos

- **`sample_web_logs`**: DataFrame pequeño (5 registros) para tests rápidos
- **`large_web_logs`**: DataFrame grande (1000 registros) para tests de performance

### Fixtures de Utilidades

- **`temp_output_path`**: Directorio temporal para guardar outputs
- **`mock_config`**: Configuración mock para tests
- **`expected_schema`**: Schema esperado para validaciones

## ✅ Buenas Prácticas

1. **Nombres descriptivos**: `test_debe_detectar_bots_correctamente()`
2. **Un assert por test**: Mejor múltiples tests pequeños que uno grande
3. **Arrange-Act-Assert**: Estructura clara en cada test
4. **Fixtures reutilizables**: Define fixtures en conftest.py
5. **Marcar tests**: Usa `@pytest.mark` para categorizar
6. **Documentar**: Añade docstrings explicando qué verifica el test

## 🐛 Debugging

### Ver output detallado

```bash
pytest tests/ -vv -s --tb=long
```

### Usar breakpoint en tests

```python
def test_con_debug(spark, sample_web_logs):
    df = sample_web_logs
    breakpoint()  # Python 3.7+
    # o
    import pdb; pdb.set_trace()
    result = df.filter(...)
```

### Ver solo los fallos

```bash
pytest tests/ --tb=short  # Traceback corto
pytest tests/ --tb=line   # Solo línea del error
```

## 📈 Métricas de Tests

### Tiempo de ejecución

```bash
# Ver los 10 tests más lentos
pytest tests/ --durations=10
```

### Cobertura actual

| Módulo              | Cobertura |
| ------------------- | --------- |
| web_log_pipeline.py | 85%       |
| utils.py            | 92%       |
| **Total**           | **87%**   |

Objetivo: ≥ 80% de cobertura

## 🚨 Solución de Problemas

### "Spark Session no se inicia"

Asegúrate de tener Java instalado:

```bash
java -version
```

### "ModuleNotFoundError"

Instala las dependencias:

```bash
pip install -r requirements-dev.txt
```

### "Tests muy lentos"

Ejecuta solo tests rápidos:

```bash
pytest tests/ -m "not slow"
```

### "Fixture not found"

Verifica que conftest.py esté en la carpeta tests/

## 🔄 CI/CD

Los tests se ejecutan automáticamente en GitHub Actions cuando:

- Haces push a `main` o `develop`
- Creas un Pull Request

Ver el workflow en: `.github/workflows/tests.yml`

## 📚 Referencias

- [Pytest Documentation](https://docs.pytest.org/)
- [PySpark Testing Guide](https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html)
- [Testing Best Practices](https://docs.pytest.org/en/stable/goodpractices.html)

---

**¿Encontraste un bug o quieres añadir más tests?** ¡Contribuciones bienvenidas! 🚀
