from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from etl.db import get_conn

app = FastAPI(title="Videolocadora API", version="0.1.0")


class FilmeCreate(BaseModel):
    nome_filme: str
    genero_filme: str
    ano_filme: int


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/filmes")
def listar_filmes(limit: int = 50, offset: int = 0):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT id, nome_filme, genero_filme, ano_filme
            FROM filmes
            ORDER BY id
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        return cur.fetchall()
    finally:
        conn.close()


@app.post("/filmes")
def criar_filme(filme: FilmeCreate):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO filmes (nome_filme, genero_filme, ano_filme)
            VALUES (%s, %s, %s)
            """,
            (filme.nome_filme, filme.genero_filme, filme.ano_filme),
        )
        conn.commit()
        return {"message": "Filme criado com sucesso", "id": cur.lastrowid}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()


@app.get("/views/faturamento-mensal")
def faturamento_mensal():
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT mes, total_locacoes, faturamento_mes
            FROM vw_faturamento_mensal
            ORDER BY mes
            """
        )
        return cur.fetchall()
    finally:
        conn.close()


@app.get("/views/filmes-ranking")
def filmes_ranking():
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT id_filme, nome_filme, total_locacoes, faturamento_total
            FROM vw_filmes_ranking
            ORDER BY faturamento_total DESC
            """
        )
        return cur.fetchall()
    finally:
        conn.close()
