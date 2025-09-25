import os
import json
import time
import glob
import csv
from datetime import datetime

from kafka import KafkaProducer  # pip install kafka-python

# =========================================================
# ENV
# =========================================================
BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9094")  # listener externo (host)
TOPIC       = os.getenv("KAFKA_TOPIC", "reclamacoes")
RAW_DIR     = os.getenv("RAW_DIR", "./data/raw")
FILE_GLOB   = os.getenv("FILE_GLOB", "*.csv")
CSV_SEP     = os.getenv("CSV_SEP", ";")          # seus arquivos vêm com ';'
CSV_ENCODING= os.getenv("CSV_ENCODING", "latin-1")  # e codificação Latin-1
FLUSH_EVERY = int(os.getenv("FLUSH_EVERY", "500"))  # flush a cada N mensagens

# =========================================================
# Helpers
# =========================================================
def now_iso():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def pick(d, keys, default=None):
    """pega o primeiro campo existente do dict d, dado uma lista de possíveis nomes"""
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default

def as_float(x):
    if x is None:
        return None
    try:
        # troca decimal brasileiro
        s = str(x).strip().replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        try:
            return float(x)
        except Exception:
            return None

def only_digits(s):
    if s is None:
        return None
    return "".join(ch for ch in str(s) if ch.isdigit()) or None

# =========================================================
# Producer
# =========================================================
def build_producer():
    print(f"[PRODUCER] bootstrap={BOOTSTRAP} | topic={TOPIC}")
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda d: json.dumps(d, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: (k if isinstance(k, bytes) else str(k).encode("utf-8")),
        linger_ms=50,
        retries=3,
        acks="all",
    )

# nomes longos presentes nos CSVs do BCB
VALOR_COLS = [
    "Quantidade total de reclamações",
    "Quantidade total de reclamações reguladas",
    "Quantidade de reclamações reguladas procedentes",
    "Quantidade de reclamações reguladas - outras",
    "Quantidade de reclamações não reguladas",
    "Quantidade total de clientes – CCS e SCR",
    "Quantidade total de clientes \u2013 CCS e SCR",  # variante com en-dash
    "Quantidade total de clientes – CCS e SCR ",
]

INSTITUICAO_COLS = [
    "instituicao", "instituição", "instituicao_financeira",
    "Instituição financeira", "Instituicao financeira", "banco", "nome"
]

CNPJ_COLS = ["cnpj", "CNPJ", "cnpj_if", "CNPJ IF", "cnpj_base"]

UF_COLS = ["uf", "UF", "estado"]

DATA_COLS = ["data_ref", "dt_referencia", "data", "mes_ano", "periodo", "Ano"]

def guess_valor(row):
    # retorna a 1ª coluna presente em VALOR_COLS
    for k in VALOR_COLS:
        if k in row and row[k] not in ("", None):
            return as_float(row[k])
    # fallback: alguns datasets têm "Índice" com vírgula
    if "Índice" in row:
        return as_float(row["Índice"])
    return None

def stream_files(producer: KafkaProducer) -> int:
    paths = sorted(glob.glob(os.path.join(RAW_DIR, FILE_GLOB)))
    if not paths:
        print(f"[PRODUCER] Nenhum arquivo encontrado em {RAW_DIR}\\{FILE_GLOB}")
        return 0

    total = 0
    for path in paths:
        try:
            if os.path.getsize(path) == 0:
                print(f"[PRODUCER] Pulando (0 KB): {os.path.basename(path)}")
                continue

            print(f"[PRODUCER] Lendo: {path}")
            with open(path, "r", encoding=CSV_ENCODING, errors="ignore", newline="") as f:
                reader = csv.DictReader(f, delimiter=CSV_SEP)
                if not reader.fieldnames:
                    print(f"[PRODUCER] Aviso: sem cabeçalho em {path}, pulando.")
                    continue

                for i, row in enumerate(reader, start=1):
                    rec = {
                        "id": f"{os.path.basename(path)}:{i}",
                        "data_ref": pick(row, DATA_COLS),
                        "instituicao": pick(row, INSTITUICAO_COLS),
                        "cnpj": only_digits(pick(row, CNPJ_COLS)),
                        "uf": pick(row, UF_COLS),
                        "valor": guess_valor(row),
                        "created_at": now_iso(),
                    }

                    key = rec["cnpj"] or rec["id"]  # partição por CNPJ se existir
                    producer.send(TOPIC, key=key, value=rec)
                    total += 1

                    if total % FLUSH_EVERY == 0:
                        producer.flush()
                producer.flush()
        except Exception as e:
            print(f"[PRODUCER] ERRO lendo {path}: {e}")

    return total

def main():
    t0 = time.time()
    producer = build_producer()
    count = stream_files(producer)
    producer.flush()
    producer.close(5)
    dt = time.time() - t0
    if count == 0:
        print(f"[PRODUCER] Nenhuma mensagem enviada (verifique RAW_DIR={RAW_DIR}).")
    else:
        print(f"[PRODUCER] Enviado {count} mensagens para {TOPIC} em {dt:.1f}s.")

if __name__ == "__main__":
    main()
