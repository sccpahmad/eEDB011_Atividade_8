import os
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, from_json, upper, trim, regexp_replace, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# =========================================================
# ENV
# =========================================================
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC     = os.getenv("KAFKA_TOPIC", "reclamacoes")

OUT_DIR   = os.getenv("OUT_DIR", "/home/jovyan/work/data/out/enriched")
CKPT_DIR  = os.getenv("CKPT_DIR") or os.getenv("CHECKPOINT_DIR") or "/home/jovyan/work/data/checkpoints/reclamacoes"

BANKS_CSV      = os.getenv("BANKS_CSV", "")
BANKS_CSV_SEP  = os.getenv("BANKS_CSV_SEP", ",")
if BANKS_CSV_SEP == r"\t":
    BANKS_CSV_SEP = "\t"
BANKS_CSV_ENC  = os.getenv("BANKS_CSV_ENCODING", "utf-8")

BANKS_CNPJ_COL = os.getenv("BANKS_CNPJ_COL", "CNPJ")
BANKS_NOME_COL = os.getenv("BANKS_NOME_COL", "Nome")
BANKS_SEG_COL  = os.getenv("BANKS_SEG_COL", "Segmento")

DEBUG_TYPES    = os.getenv("DEBUG_TYPES", "0") == "1"

# =========================================================
# Spark
# =========================================================
spark = (
    SparkSession.builder.appName("atividade8-consumer")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(f"[STREAM] Kafka: {BOOTSTRAP} | tópico: {TOPIC}")
print(f"[STREAM] Saída : {OUT_DIR}")
print(f"[STREAM] Ckpt  : {CKPT_DIR}")
print(f"[STREAM] Banks : path={BANKS_CSV} sep={repr(BANKS_CSV_SEP)} enc={BANKS_CSV_ENC}")

# =========================================================
# Dimensão de Bancos
# =========================================================
def load_banks_dim() -> Optional[DataFrame]:
    if not BANKS_CSV:
        print("[BANKS] Nenhum arquivo informado; dimensão vazia.")
        return None
    try:
        df = (
            spark.read.option("header", True)
            .option("encoding", BANKS_CSV_ENC)
            .option("sep", BANKS_CSV_SEP)
            .csv(BANKS_CSV)
        )
        cols = df.columns
        cnpj_col = BANKS_CNPJ_COL if BANKS_CNPJ_COL in cols else next((c for c in cols if c.lower()=="cnpj"), None)
        nome_col = BANKS_NOME_COL if BANKS_NOME_COL in cols else next((c for c in cols if c.lower() in ("nome","nome_original")), None)
        seg_col  = BANKS_SEG_COL  if BANKS_SEG_COL  in cols else next((c for c in cols if c.lower()=="segmento"), None)

        keep = [c for c in [cnpj_col, nome_col, seg_col] if c]
        df = df.select(*keep)

        if cnpj_col:
            df = df.withColumn(cnpj_col, regexp_replace(col(cnpj_col).cast(StringType()), r"[^0-9]", ""))
        if nome_col:
            df = df.withColumn(nome_col, trim(upper(col(nome_col))).cast(StringType()))
        if seg_col:
            df = df.withColumn(seg_col, trim(col(seg_col)).cast(StringType()))

        # Padroniza nomes que usaremos no join
        if cnpj_col: df = df.withColumnRenamed(cnpj_col, "bank_cnpj")
        if nome_col: df = df.withColumnRenamed(nome_col, "bank_nome")
        if seg_col:  df = df.withColumnRenamed(seg_col,  "bank_segmento")

        return df.dropDuplicates()
    except Exception as e:
        print(f"[BANKS] ERRO ao ler dimensão: {e}")
        return None

banks_dim = load_banks_dim()
if banks_dim is None:
    schema_empty = StructType([
        StructField("bank_cnpj", StringType(), True),
        StructField("bank_nome", StringType(), True),
        StructField("bank_segmento",  StringType(), True),
    ])
    banks_dim = spark.createDataFrame([], schema_empty)

# =========================================================
# Esquema esperado da mensagem
# =========================================================
msg_schema = StructType([
    StructField("id", StringType(), True),
    StructField("data_ref", StringType(), True),
    StructField("instituicao", StringType(), True),
    StructField("cnpj", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("valor", DoubleType(), True),
    StructField("created_at", StringType(), True),
])

# =========================================================
# Fonte: Kafka
# =========================================================
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

decoded = raw.selectExpr("CAST(value AS STRING) as value") \
             .select(from_json(col("value"), msg_schema).alias("j")).select("j.*")

# Normalizações para join
enriched_input = (
    decoded
    .withColumn("id",         col("id").cast(StringType()))
    .withColumn("data_ref",   col("data_ref").cast(StringType()))
    .withColumn("instituicao",trim(upper(col("instituicao"))).cast(StringType()))
    .withColumn("cnpj",       regexp_replace(trim(col("cnpj").cast(StringType())), r"[^0-9]", ""))
    .withColumn("uf",         trim(upper(col("uf"))).cast(StringType()))
    .withColumn("valor",      col("valor").cast(DoubleType()))
    .withColumn("cnpj_norm",  col("cnpj"))
    .withColumn("nome_norm",  col("instituicao"))
)

# =========================================================
# Enriquecimento sem ambiguidade
# =========================================================
def enrich_and_write(microbatch_df: DataFrame, batch_id: int):
    in_count = microbatch_df.count()
    if in_count == 0:
        print(f"[BATCH {batch_id:05d}] vazio (entrada=0)")
        return

    m = microbatch_df.alias("m")

    # Dimensão de CNPJ (só o que precisamos) com nomes únicos
    b_cnpj = banks_dim.select(
        col("bank_cnpj").alias("b_cnpj_cnpj"),
        col("bank_segmento").alias("seg_from_cnpj")
    ).alias("b_cnpj")

    # Dimensão de Nome (só o que precisamos) com nomes únicos
    b_nome = banks_dim.select(
        col("bank_nome").alias("b_nome_nome"),
        col("bank_segmento").alias("seg_from_nome")
    ).alias("b_nome")

    # 1º join por CNPJ -> mantém apenas cols do microbatch + seg_from_cnpj
    df1 = (
        m.join(b_cnpj, m["cnpj_norm"] == col("b_cnpj.b_cnpj_cnpj"), "left")
         .select(m["*"], col("b_cnpj.seg_from_cnpj"))
    )

    # 2º join por NOME -> mantém apenas cols do df1 + seg_from_nome
    df2 = (
        df1.join(b_nome, df1["nome_norm"] == col("b_nome.b_nome_nome"), "left")
           .select(df1["*"], col("b_nome.seg_from_nome"))
    )

    df2 = df2.withColumn(
        "segmento_final",
        coalesce(col("seg_from_cnpj"), col("seg_from_nome"))
    )

    # saída
    df_out = df2.select(
        col("id"),
        col("data_ref"),
        col("instituicao"),
        col("cnpj"),
        col("uf"),
        col("valor"),
        col("segmento_final"),
        current_timestamp().alias("processed_at"),
    )

    fill_str = {c: "" for c in ["id", "data_ref", "instituicao", "cnpj", "uf", "segmento_final"]}
    df_out = df_out.na.fill(fill_str)

    if DEBUG_TYPES:
        print(f"[BATCH {batch_id:05d}] Schema de saída:")
        df_out.printSchema()

    out_path = f"{OUT_DIR}/batch_{batch_id:05d}"
    (
        df_out.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .option("emptyValue", "")
        .option("nullValue", "")
        .csv(out_path)
    )
    print(f"[BATCH {batch_id:05d}] entrada={in_count} -> escrito em {out_path}")

query = (
    enriched_input.writeStream
    .foreachBatch(enrich_and_write)
    .option("checkpointLocation", CKPT_DIR)
    .outputMode("update")
    .start()
)

query.awaitTermination()
