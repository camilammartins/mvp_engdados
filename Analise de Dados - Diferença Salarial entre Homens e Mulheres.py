# Databricks notebook source
from pyspark.sql.functions import substring, col

# 1) Ler a tabela bruta (a que veio do TXT de 1,65GB)
df_raw = spark.table("pnadc_042023")   # se o nome for outro, troque aqui

# 2) Criar colunas estruturadas a partir da coluna "value"
#    As posições vêm do input_PNADC_trimestral.txt:
#    @0001 Ano      $4.
#    @0005 Trimestre $1.
#    @0006 UF       $2.

df_struct = (
    df_raw
    .select(
        substring("value", 1, 4).alias("ano"),
        substring("value", 5, 1).alias("trimestre"),
        substring("value", 6, 2).alias("uf")
        # 👉 aqui depois nós vamos acrescentar sexo, idade, escolaridade, renda
        #    usando o mesmo padrão, com as posições que estão no input_PNADC_trimestral.txt
    )
    # 3) Converter tipos para numérico onde faz sentido
    .withColumn("ano", col("ano").cast("int"))
    .withColumn("trimestre", col("trimestre").cast("int"))
    .withColumn("uf", col("uf").cast("int"))
)

# 4) Ver algumas linhas para conferir
display(df_struct.limit(20))

# 5) Gravar como tabela "curated" em Delta
df_struct.write.format("delta").mode("overwrite").saveAsTable("pnadc_042023_curated")

# COMMAND ----------

from pyspark.sql.functions import substring, col

# 1) Ler a tabela bruta
df_raw = spark.table("pnadc_042023")   # ajuste se o nome for diferente

# 2) Construir colunas a partir da coluna "value"
df_struct = (
    df_raw
    .select(
        substring("value", 1, 4).alias("ano"),          # @0001 Ano
        substring("value", 5, 1).alias("trimestre"),    # @0005 Trimestre
        substring("value", 6, 2).alias("uf"),           # @0006 UF
        
        # 👉 Renda habitual (VD4019)
        substring("value", 444, 8).alias("renda_raw")
    )
    # 3) converter tipos
    .withColumn("ano", col("ano").cast("int"))
    .withColumn("trimestre", col("trimestre").cast("int"))
    .withColumn("uf", col("uf").cast("int"))
    .withColumn("renda_raw", col("renda_raw").cast("double"))
    # 4) ajustar se estiver em centavos (provavelmente sim)
    .withColumn("renda", col("renda_raw") / 100)
)

display(df_struct.limit(20))

# COMMAND ----------

from pyspark.sql.functions import substring

df_tmp = df_raw.select(
    substring("value", 1, 4).alias("ano"),
    substring("value", 5, 1).alias("trimestre"),
    substring("value", 6, 2).alias("uf"),
    substring("value", 444, 8).alias("renda_raw")  # posição do VD4019
)

display(df_tmp.limit(20))

# COMMAND ----------

from pyspark.sql.functions import col, trim, regexp_replace, when

df_struct = (
    df_tmp
    # converter ano, trimestre, uf pra número
    .withColumn("ano", col("ano").cast("int"))
    .withColumn("trimestre", col("trimestre").cast("int"))
    .withColumn("uf", col("uf").cast("int"))
    # limpar a renda: tirar espaços e qualquer coisa que não seja dígito
    .withColumn(
        "renda_num_str",
        regexp_replace(trim(col("renda_raw")), "[^0-9]", "")
    )
    # converter para double; se string ficar vazia, vira NULL
    .withColumn(
        "renda_bruta",
        when(col("renda_num_str") == "", None)
        .otherwise(col("renda_num_str").cast("double") / 100)  # divide por 100 (centavos -> reais)
    )
)

display(df_struct.limit(20))

# COMMAND ----------

df_struct.write.format("delta").mode("overwrite").saveAsTable("pnadc_042023_curated")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS pnadc_042023_curated")

# COMMAND ----------

df_struct.write.format("delta").mode("overwrite").saveAsTable("pnadc_042023_curated")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ano, trimestre, uf, renda_bruta
# MAGIC FROM pnadc_042023_curated
# MAGIC LIMIT 20;

# COMMAND ----------

from pyspark.sql.functions import substring, col, trim, regexp_replace, when

df_struct = (
    df_raw
    .select(
        substring("value", 1, 4).alias("ano"),          # @0001
        substring("value", 5, 1).alias("trimestre"),    # @0005
        substring("value", 6, 2).alias("uf"),           # @0006

        substring("value", 95, 1).alias("sexo"),        # @0095 V2007

        substring("value", 444, 8).alias("renda_raw")   # @0444 VD4019
    )
    # converter tipos básicos
    .withColumn("ano", col("ano").cast("int"))
    .withColumn("trimestre", col("trimestre").cast("int"))
    .withColumn("uf", col("uf").cast("int"))
    .withColumn("sexo", col("sexo").cast("int"))
    
    # limpar renda
    .withColumn("renda_num_str", regexp_replace(trim(col("renda_raw")), "[^0-9]", ""))
    .withColumn(
        "renda_bruta",
        when(col("renda_num_str") == "", None)
        .otherwise(col("renda_num_str").cast("double") / 100)
    )
)

display(df_struct.limit(20))

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS pnadc_042023_curated")
df_struct.write.format("delta").mode("overwrite").saveAsTable("pnadc_042023_curated")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sexo, avg(renda_bruta)
# MAGIC FROM pnadc_042023_curated
# MAGIC GROUP BY sexo;

# COMMAND ----------

from pyspark.sql.functions import substring, col, trim, regexp_replace, when

# 1) Ler a tabela bruta
df_raw = spark.table("pnadc_042023")   # mesmo nome de antes

# 2) Construir colunas a partir de "value"
df_struct = (
    df_raw
    .select(
        substring("value", 1, 4).alias("ano"),           # @0001 Ano
        substring("value", 5, 1).alias("trimestre"),     # @0005 Trimestre
        substring("value", 6, 2).alias("uf"),            # @0006 UF

        substring("value", 95, 1).alias("sexo"),         # @0095 V2007 (Sexo)

        substring("value", 104, 3).alias("idade_raw"),   # @0104 V2009 (Idade)
        substring("value", 125, 2).alias("esc_cod_raw"), # @0125 V3009A (Escolaridade)
        substring("value", 107, 1).alias("raca_cod_raw"),# @0107 V2010 (Cor/raça)

        substring("value", 444, 8).alias("renda_raw")    # @0444 VD4019 (Renda habitual)
    )
    # 3) Converter tipos e limpar
    .withColumn("ano", col("ano").cast("int"))
    .withColumn("trimestre", col("trimestre").cast("int"))
    .withColumn("uf", col("uf").cast("int"))
    .withColumn("sexo", col("sexo").cast("int"))

    .withColumn("idade", col("idade_raw").cast("int"))
    .withColumn("esc_cod", trim(col("esc_cod_raw")))
    .withColumn("raca_cod", col("raca_cod_raw").cast("int"))

    .withColumn("renda_num_str", regexp_replace(trim(col("renda_raw")), "[^0-9]", ""))
    .withColumn(
        "renda_bruta",
        when(col("renda_num_str") == "", None)
        .otherwise(col("renda_num_str").cast("double") / 100)
    )
)

display(df_struct.limit(20))

# COMMAND ----------

# apagar tabela antiga, se existir
spark.sql("DROP TABLE IF EXISTS pnadc_042023_curated")

# salvar a nova versão com todas as colunas
df_struct.write.format("delta").mode("overwrite").saveAsTable("pnadc_042023_curated")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ano, trimestre, uf, sexo, idade, esc_cod, raca_cod, renda_bruta
# MAGIC FROM pnadc_042023_curated
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sexo, AVG(renda_bruta)
# MAGIC FROM pnadc_042023_curated
# MAGIC GROUP BY sexo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sexo, COUNT(*) AS qtd_pessoas
# MAGIC FROM pnadc_042023_curated
# MAGIC GROUP BY sexo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   sexo,
# MAGIC   AVG(renda_bruta)   AS renda_media,
# MAGIC   PERCENTILE(renda_bruta, 0.5) AS renda_mediana,
# MAGIC   PERCENTILE(renda_bruta, 0.1) AS p10,
# MAGIC   PERCENTILE(renda_bruta, 0.9) AS p90
# MAGIC FROM pnadc_042023_curated
# MAGIC WHERE renda_bruta IS NOT NULL
# MAGIC GROUP BY sexo;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH medias AS (
# MAGIC   SELECT 
# MAGIC     sexo,
# MAGIC     AVG(renda_bruta) AS renda_media
# MAGIC   FROM pnadc_042023_curated
# MAGIC   WHERE renda_bruta IS NOT NULL
# MAGIC   GROUP BY sexo
# MAGIC )
# MAGIC SELECT
# MAGIC   MAX(CASE WHEN sexo = 1 THEN renda_media END) AS renda_homem,
# MAGIC   MAX(CASE WHEN sexo = 2 THEN renda_media END) AS renda_mulher,
# MAGIC   1 - (MAX(CASE WHEN sexo = 2 THEN renda_media END) / MAX(CASE WHEN sexo = 1 THEN renda_media END)) 
# MAGIC     AS gap_relativo
# MAGIC FROM medias;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   uf,
# MAGIC   sexo,
# MAGIC   AVG(renda_bruta) AS renda_media
# MAGIC FROM pnadc_042023_curated
# MAGIC WHERE renda_bruta IS NOT NULL
# MAGIC GROUP BY uf, sexo
# MAGIC ORDER BY uf, sexo;