-- Commands --

# Leer datos desde un archivo CSV:

df = spark.read.csv("ruta_del_archivo.csv", header=True, inferSchema=True)
# Leer datos desde un archivo JSON:
df = spark.read.json("ruta_del_archivo.json")

# Leer datos desde una base de datos JDBC:
df = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost/dbname").option("dbtable", "tablename").option("user", "username").option("password", "password").load()

# Mostrar las primeras filas de un DataFrame:
df.show()

# Obtener el número de filas en un DataFrame:
df.count()

# Filtrar filas basadas en una condición:
df_filtered = df.filter(df.columna > valor)

# Seleccionar columnas específicas:
df_selected = df.select("columna1", "columna2")

# Renombrar una columna:
df_renamed = df.withColumnRenamed("old_column", "new_column")

# Ordenar un DataFrame:
df_sorted = df.orderBy("columna")

# Agrupar datos y realizar agregaciones:
df_grouped = df.groupBy("columna").agg({"columna2": "sum", "columna3": "avg"})

# Unir dos DataFrames:
df_union = df1.union(df2)

# Unir dos DataFrames por una columna común:
df_join = df1.join(df2, df1.columna_comun == df2.columna_comun)

# Agregar una nueva columna basada en una expresión:
df_new_column = df.withColumn("nueva_columna", df.columna1 + df.columna2)

# Eliminar una columna:
df_removed_column = df.drop("columna")

# Eliminar filas duplicadas:
df_no_duplicates = df.dropDuplicates()

# Filtrar filas nulas:
df_no_nulls = df.dropna()

# -- Operaciones Avanzadas --

# Crear un DataFrame a partir de un RDD:
rdd = sc.parallelize([(1, 'a'), (2, 'b'), (3, 'c')])
df_from_rdd = spark.createDataFrame(rdd, ['id', 'letra'])

# Convertir un DataFrame a RDD:
rdd = df.rdd

# Ejecutar una consulta SQL en un DataFrame:
df.createOrReplaceTempView("tabla")
result = spark.sql("SELECT * FROM tabla WHERE columna > 10")

# Manejo de Errores y Depuración
# Manejar errores con Try-Except:
try:
    df_operation()
except Exception as e:
    print("Ocurrió un error:", e)

# Registrar mensajes de depuración:
spark.sparkContext.setLogLevel("DEBUG")

# Otros Comandos Útiles

# Obtener el esquema de un DataFrame:
df.printSchema()

# Obtener estadísticas descriptivas:
df.describe().show()

# Obtener una muestra aleatoria de un DataFrame:
df_sample = df.sample(fraction=0.1, seed=42)

# Convertir un DataFrame a Pandas DataFrame:
pandas_df = df.toPandas()

# Convertir un Pandas DataFrame a PySpark DataFrame:
df = spark.createDataFrame(pandas_df)

# Ejecutar código en el cluster:
spark.sparkContext.parallelize(range(1000)).foreach(lambda x: x ** 2)

# Detener la sesión de Spark:
spark.stop()
