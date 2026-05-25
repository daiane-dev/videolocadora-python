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
    return s[:2]


def main(csv_path: str):
    df = pd.read_csv(csv_path)

    required = {"nome_usuario", "idade", "cidade", "estado"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV faltando colunas: {missing}")

    df["nome_usuario"] = df["nome_usuario"].apply(clean_str)
    df["cidade"] = df["cidade"].apply(clean_str)
    df["estado"] = df["estado"].apply(clean_uf)
    df["idade"] = pd.to_numeric(df["idade"], errors="coerce")

    df = df[df["nome_usuario"].notna()].copy()
    df = df[df["idade"].notna()].copy()
    df["idade"] = df["idade"].astype(int)

    conn = get_conn()
    try:
        cur = conn.cursor()

        # Busca usuários já existentes no banco
        cur.execute("SELECT nome_usuario, idade, cidade, estado FROM usuarios")
        usuarios_existentes = {
            (
                nome.strip().lower(),
                int(idade),
                cidade.strip().lower() if cidade else None,
                estado.strip().upper() if estado else None,
            )
            for nome, idade, cidade, estado in cur.fetchall()
        }

        rows = []
        duplicados = 0

        for r in df.itertuples(index=False):
            chave = (
                r.nome_usuario.strip().lower(),
                int(r.idade),
                r.cidade.strip().lower() if r.cidade else None,
                r.estado.strip().upper() if r.estado else None,
            )

            if chave in usuarios_existentes:
                duplicados += 1
                continue

            rows.append((r.nome_usuario, r.idade, r.cidade, r.estado))
            usuarios_existentes.add(chave)

        if not rows:
            print(f"Nenhum usuário novo para inserir. Duplicados ignorados: {duplicados}")
            return

        sql = """
            INSERT INTO usuarios (nome_usuario, idade, cidade, estado)
            VALUES (%s, %s, %s, %s)
        """

        cur.executemany(sql, rows)
        conn.commit()

        print(f"OK: {len(rows)} usuários inseridos com sucesso.")
        print(f"Duplicados ignorados: {duplicados}")

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