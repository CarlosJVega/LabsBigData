# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 5 - Crypto Summary Job
# MAGIC
# MAGIC Este notebook forma parte de un Databricks Bundle. Su objetivo es demostrar como un flujo simple de analitica puede pasar de ser un notebook manual a un recurso ejecutable por un Job declarado como codigo.
# MAGIC
# MAGIC El flujo realiza:
# MAGIC
# MAGIC 1. Lectura de una tabla Delta fuente.
# MAGIC 2. Calculo de metricas resumen por moneda.
# MAGIC 3. Escritura de una tabla Delta de salida.
# MAGIC 4. Validacion basica del resultado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parametros del job
# MAGIC
# MAGIC Los parametros se reciben desde el archivo `resources/jobs.yml` del bundle.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "bigdata")
dbutils.widgets.text("source_table", "workspace.bigdata.crypto_prices_lab3_gold")
dbutils.widgets.text("output_table", "workspace.bigdata.crypto_bundle_summary")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_table = dbutils.widgets.get("source_table")
output_table = dbutils.widgets.get("output_table")

print(f"Catalogo: {catalog}")
print(f"Esquema: {schema}")
print(f"Tabla fuente: {source_table}")
print(f"Tabla salida: {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verificacion de entorno
# MAGIC
# MAGIC Se crea el esquema si no existe y se valida que la tabla fuente este disponible.

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

def table_exists(table_name: str) -> bool:
    try:
        spark.sql(f"SELECT 1 FROM {table_name} LIMIT 1").collect()
        return True
    except Exception:
        return False

if not table_exists(source_table):
    raise ValueError(
        f"La tabla fuente '{source_table}' no existe o no se puede leer. "
        "Ejecuta primero el Lab 3 o ajusta la variable source_table del bundle."
    )

print("Tabla fuente validada correctamente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lectura de la tabla fuente

# COMMAND ----------

df = spark.table(source_table)

display(df.limit(20))
print(f"Total de registros fuente: {df.count()}")
print("Columnas disponibles:")
print(df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Preparacion de columnas
# MAGIC
# MAGIC Este bloque permite que el notebook funcione con distintas tablas de los laboratorios, siempre que tengan una columna `coin`, una columna de precio y alguna columna de volumen.

# COMMAND ----------

columns = set(df.columns)

if "coin" not in columns:
    raise ValueError("La tabla fuente debe contener la columna 'coin'.")

price_col = "price" if "price" in columns else None
if price_col is None:
    raise ValueError("La tabla fuente debe contener una columna 'price'.")

if "total_volume" in columns:
    volume_col = "total_volume"
elif "volume" in columns:
    volume_col = "volume"
else:
    volume_col = None

if "date" in columns:
    time_col = "date"
elif "event_time" in columns:
    time_col = "event_time"
elif "window_start" in columns:
    time_col = "window_start"
else:
    time_col = None

print(f"Columna de precio: {price_col}")
print(f"Columna de volumen: {volume_col}")
print(f"Columna temporal: {time_col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Calculo de resumen analitico
# MAGIC
# MAGIC Se calcula una tabla resumen por moneda. Esta tabla representa un recurso derivado que podria ser usado por analistas, dashboards o jobs posteriores.

# COMMAND ----------

agg_exprs = [
    F.count("*").alias("total_records"),
    F.round(F.avg(F.col(price_col)), 4).alias("avg_price"),
    F.round(F.min(F.col(price_col)), 4).alias("min_price"),
    F.round(F.max(F.col(price_col)), 4).alias("max_price"),
]

if volume_col is not None:
    agg_exprs.extend([
        F.round(F.avg(F.col(volume_col)), 2).alias("avg_volume"),
        F.round(F.sum(F.col(volume_col)), 2).alias("total_volume"),
    ])

if time_col is not None:
    agg_exprs.extend([
        F.min(F.col(time_col)).alias("min_time"),
        F.max(F.col(time_col)).alias("max_time"),
    ])

summary_df = (
    df.groupBy("coin")
    .agg(*agg_exprs)
    .withColumn("processed_at", F.current_timestamp())
)

display(summary_df.orderBy("coin"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Escritura de la tabla Delta de salida

# COMMAND ----------

(
    summary_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(output_table)
)

print(f"Tabla de salida creada o actualizada: {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validacion final

# COMMAND ----------

result_df = spark.table(output_table)

display(result_df.orderBy("coin"))
print(f"Total de monedas resumidas: {result_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Cierre
# MAGIC
# MAGIC Este notebook demostro como un flujo simple puede ejecutarse como parte de un Job declarado en un Databricks Bundle.

# COMMAND ----------

print("Job ejecutado correctamente.")
print(f"Fuente: {source_table}")
print(f"Salida: {output_table}")

