import sys
import pandas as pd
from etl.db import get_conn

def clean_str(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    return s if s else None

def clean_uf(x):
    s = clean_str(x)
    if not s:
        return None
    s = s.upper()
    return s[:2]  # garante 2 letras (SP, RJ...)

def main(csv_path: str):
    df = pd.read_csv(csv_path)

    # 1) validação de colunas
    required = {"nome_usuario", "idade", "cidade", "estado"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV faltando colunas: {missing}")

    # 2) limpeza e padronização
    df["nome_usuario"] = df["nome_usuario"].apply(clean_str)
    df["cidade"] = df["cidade"].apply(clean_str)
    df["estado"] = df["estado"].apply(clean_uf)
    df["idade"] = pd.to_numeric(df["idade"], errors="coerce")

    # 3) remove linhas inválidas (nome vazio ou idade inválida)
    df = df[df["nome_usuario"].notna()].copy()
    df = df[df["idade"].notna()].copy()

    # 4) converte idade para int
    df["idade"] = df["idade"].astype(int)

    rows = [
        (r.nome_usuario, r.idade, r.cidade, r.estado)
        for r in df.itertuples(index=False)
    ]

    sql = """
        INSERT INTO usuarios (nome_usuario, idade, cidade, estado)
        VALUES (%s, %s, %s, %s)
    """

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.executemany(sql, rows)
        conn.commit()
        print(f"OK: {len(rows)} usuários inseridos com sucesso.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python3 -m etl.load_usuarios data/usuarios.csv")
        raise SystemExit(1)
    main(sys.argv[1])
