import sys
import pandas as pd
from etl.db import get_conn

def main(csv_path: str, reset: bool = True):
    df = pd.read_csv(csv_path)

    # 1) validação de colunas
    required = {
        "id_usuario",
        "id_filme",
        "data_locacao",
        "data_prevista_locacao",
        "data_devolucao",
        "valor_diaria",
        "dias",
        "status_locacao",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV faltando colunas: {missing}")

    # 2) limpeza / conversões
    df["id_usuario"] = pd.to_numeric(df["id_usuario"], errors="coerce")
    df["id_filme"] = pd.to_numeric(df["id_filme"], errors="coerce")
    df["valor_diaria"] = pd.to_numeric(df["valor_diaria"], errors="coerce")
    df["dias"] = pd.to_numeric(df["dias"], errors="coerce")

    # Datas: converte para datetime; erros viram NaT
    df["data_locacao"] = pd.to_datetime(df["data_locacao"], errors="coerce").dt.date
    df["data_prevista_locacao"] = pd.to_datetime(df["data_prevista_locacao"], errors="coerce").dt.date

    # data_devolucao pode ser vazia: vira NaT/NaN -> None
    df["data_devolucao"] = pd.to_datetime(df["data_devolucao"], errors="coerce").dt.date

    # status: padroniza
    df["status_locacao"] = df["status_locacao"].astype(str).str.strip().str.upper()

    # 3) remove linhas inválidas básicas
    df = df[df["id_usuario"].notna() & df["id_filme"].notna()].copy()
    df = df[df["data_locacao"].notna() & df["data_prevista_locacao"].notna()].copy()
    df = df[df["valor_diaria"].notna() & df["dias"].notna()].copy()

    df["id_usuario"] = df["id_usuario"].astype(int)
    df["id_filme"] = df["id_filme"].astype(int)
    df["dias"] = df["dias"].astype(int)

    # 4) validação de FK (só insere se existir no banco)
    conn = get_conn()
    try:
        cur = conn.cursor()

        cur.execute("SELECT id FROM usuarios")
        usuarios_validos = {row[0] for row in cur.fetchall()}

        cur.execute("SELECT id FROM filmes")
        filmes_validos = {row[0] for row in cur.fetchall()}

        before = len(df)
        df = df[df["id_usuario"].isin(usuarios_validos) & df["id_filme"].isin(filmes_validos)].copy()
        after = len(df)

        if after < before:
            print(f"Aviso: {before - after} linhas removidas por FK inválida (id_usuario/id_filme não existe).")

        # 5) regra simples: se ABERTA, data_devolucao deve ser NULL
        df.loc[df["status_locacao"] == "ABERTA", "data_devolucao"] = None

        # 6) calcula valor_total
        df["valor_total"] = (df["valor_diaria"] * df["dias"]).round(2)

        # 7) modo RESET: apaga tudo antes (evita duplicação)
        if reset:
            cur.execute("DELETE FROM locacoes")
            conn.commit()
            print("RESET: tabela locacoes limpa (DELETE).")

        # 8) montar linhas e inserir
        rows = [
            (
                int(r.id_usuario),
                int(r.id_filme),
                r.data_locacao,
                r.data_prevista_locacao,
                (None if pd.isna(r.data_devolucao) else r.data_devolucao),
                float(r.valor_diaria),
                int(r.dias),
                float(r.valor_total),
                str(r.status_locacao),
            )
            for r in df.itertuples(index=False)
        ]

        sql = """
            INSERT INTO locacoes
            (id_usuario, id_filme, data_locacao, data_prevista_locacao, data_devolucao,
             valor_diaria, dias, valor_total, status_locacao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cur.executemany(sql, rows)
        conn.commit()
        print(f"OK: {len(rows)} locações inseridas com sucesso.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python3 -m etl.load_locacoes data/locacoes.csv")
        raise SystemExit(1)

    main(sys.argv[1], reset=True)
    