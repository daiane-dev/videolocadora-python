import sys
import pandas as pd
from etl.db import get_conn


def clean_str(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    return s if s else None


def main(csv_path):
    df = pd.read_csv(csv_path)

    required = {"nome_filme", "genero_filme", "ano_filme"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV faltando colunas: {missing}")

    df["nome_filme"] = df["nome_filme"].apply(clean_str)
    df["genero_filme"] = df["genero_filme"].apply(clean_str)
    df["ano_filme"] = pd.to_numeric(df["ano_filme"], errors="coerce")

    df = df[df["nome_filme"].notna()].copy()

    conn = get_conn()
    try:
        cur = conn.cursor()

        # Busca filmes já existentes no banco
        cur.execute("SELECT nome_filme, ano_filme FROM filmes")
        filmes_existentes = {
            (nome.strip().lower(), ano)
            for nome, ano in cur.fetchall()
        }

        rows = []
        duplicados = 0

        for r in df.itertuples(index=False):
            ano = None if pd.isna(r.ano_filme) else int(r.ano_filme)
            chave = (r.nome_filme.strip().lower(), ano)

            if chave in filmes_existentes:
                duplicados += 1
                continue

            rows.append((r.nome_filme, r.genero_filme, ano))
            filmes_existentes.add(chave)

        if not rows:
            print(f"Nenhum filme novo para inserir. Duplicados ignorados: {duplicados}")
            return

        sql = """
            INSERT INTO filmes (nome_filme, genero_filme, ano_filme)
            VALUES (%s, %s, %s)
        """

        cur.executemany(sql, rows)
        conn.commit()

        print(f"OK: {len(rows)} filmes inseridos com sucesso.")
        print(f"Duplicados ignorados: {duplicados}")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python3 -m etl.load_filmes data/filmes.csv")
        sys.exit(1)

    main(sys.argv[1])
