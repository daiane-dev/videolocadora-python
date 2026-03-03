from fastapi import APIRouter
from etl.db import get_conn

router = APIRouter(prefix="/views", tags=["Views"])


@router.get("/filmes-ranking")
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


@router.get("/faturamento-mensal")
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