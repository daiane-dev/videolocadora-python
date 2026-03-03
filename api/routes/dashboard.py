from fastapi import APIRouter, HTTPException
from typing import Optional, Tuple
from datetime import date, datetime

from etl.db import get_conn

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])


def parse_periodo(data_ini: Optional[str], data_fim: Optional[str]) -> Tuple[Optional[date], Optional[date]]:
    """
    Valida e converte data_ini/data_fim (YYYY-MM-DD).
    Regras:
    - ou manda as duas, ou não manda nenhuma
    - data_ini <= data_fim
    """
    if (data_ini and not data_fim) or (data_fim and not data_ini):
        raise HTTPException(
            status_code=400,
            detail="Envie data_ini e data_fim juntos (YYYY-MM-DD), ou não envie nenhum."
        )

    if not data_ini and not data_fim:
        return None, None

    try:
        dt_ini = datetime.strptime(data_ini, "%Y-%m-%d").date()
        dt_fim = datetime.strptime(data_fim, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Datas inválidas. Use o formato YYYY-MM-DD.")

    if dt_ini > dt_fim:
        raise HTTPException(status_code=400, detail="data_ini não pode ser maior que data_fim.")

    return dt_ini, dt_fim


@router.get("/resumo")
def dashboard_resumo():
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)

        hoje = date.today()

        # Totais
        cur.execute("SELECT COUNT(*) AS total_usuarios FROM usuarios")
        total_usuarios = int(cur.fetchone()["total_usuarios"])

        cur.execute("SELECT COUNT(*) AS total_filmes_ativos FROM filmes WHERE ativo = 1")
        total_filmes_ativos = int(cur.fetchone()["total_filmes_ativos"])

        cur.execute("SELECT COUNT(*) AS total_filmes_inativos FROM filmes WHERE ativo = 0")
        total_filmes_inativos = int(cur.fetchone()["total_filmes_inativos"])

        cur.execute("SELECT COUNT(*) AS total_locacoes FROM locacoes")
        total_locacoes = int(cur.fetchone()["total_locacoes"])

        # Locações abertas
        cur.execute("SELECT COUNT(*) AS locacoes_abertas FROM locacoes WHERE status_locacao = 'ABERTA'")
        locacoes_abertas = int(cur.fetchone()["locacoes_abertas"])

        # Locações atrasadas
        cur.execute(
            """
            SELECT COUNT(*) AS locacoes_atrasadas
            FROM locacoes
            WHERE status_locacao = 'ABERTA'
              AND data_prevista_locacao < %s
            """,
            (hoje,),
        )
        locacoes_atrasadas = int(cur.fetchone()["locacoes_atrasadas"])

        # Faturamento total
        cur.execute("SELECT COALESCE(SUM(valor_total), 0) AS faturamento_total FROM locacoes")
        faturamento_total = float(cur.fetchone()["faturamento_total"])

        # Faturamento do mês atual
        cur.execute(
            """
            SELECT COALESCE(SUM(valor_total), 0) AS faturamento_mes_atual
            FROM locacoes
            WHERE DATE_FORMAT(data_locacao, '%Y-%m') = DATE_FORMAT(CURDATE(), '%Y-%m')
            """
        )
        faturamento_mes_atual = float(cur.fetchone()["faturamento_mes_atual"])

        return {
            "total_usuarios": total_usuarios,
            "total_filmes_ativos": total_filmes_ativos,
            "total_filmes_inativos": total_filmes_inativos,
            "total_locacoes": total_locacoes,
            "locacoes_abertas": locacoes_abertas,
            "locacoes_atrasadas": locacoes_atrasadas,
            "faturamento_total": faturamento_total,
            "faturamento_mes_atual": faturamento_mes_atual,
        }
    finally:
        conn.close()


@router.get("/faturamento")
def faturamento_por_periodo(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    """
    Resumo de faturamento por período.
    - Se data_ini e data_fim NÃO forem enviados: considera TODAS as locações.
    - Se um for enviado, o outro também deve ser enviado.
    Formato: YYYY-MM-DD
    """
    dt_ini, dt_fim = parse_periodo(data_ini, data_fim)

    where = ""
    params = []
    if dt_ini and dt_fim:
        where = "WHERE data_locacao BETWEEN %s AND %s"
        params = [dt_ini, dt_fim]

    hoje = date.today()

    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)

        # Total de locações e faturamento
        cur.execute(
            f"""
            SELECT
              COUNT(*) AS total_locacoes,
              COALESCE(SUM(valor_total), 0) AS faturamento_total
            FROM locacoes
            {where}
            """,
            tuple(params),
        )
        resumo = cur.fetchone()

        # Abertas
        cur.execute(
            f"""
            SELECT COUNT(*) AS locacoes_abertas
            FROM locacoes
            {where + (" AND" if where else "WHERE")}
              status_locacao = 'ABERTA'
            """,
            tuple(params),
        )
        abertas = cur.fetchone()

        # Devolvidas
        cur.execute(
            f"""
            SELECT COUNT(*) AS locacoes_devolvidas
            FROM locacoes
            {where + (" AND" if where else "WHERE")}
              status_locacao = 'DEVOLVIDA'
            """,
            tuple(params),
        )
        devolvidas = cur.fetchone()

        # Atrasadas
        cur.execute(
            f"""
            SELECT COUNT(*) AS locacoes_atrasadas
            FROM locacoes
            {where + (" AND" if where else "WHERE")}
              status_locacao = 'ABERTA'
              AND data_prevista_locacao < %s
            """,
            tuple(params + [hoje]),
        )
        atrasadas = cur.fetchone()

        return {
            "periodo": {
                "data_ini": str(dt_ini) if dt_ini else None,
                "data_fim": str(dt_fim) if dt_fim else None,
            },
            "total_locacoes": int(resumo["total_locacoes"]),
            "faturamento_total": float(resumo["faturamento_total"]),
            "locacoes_abertas": int(abertas["locacoes_abertas"]),
            "locacoes_devolvidas": int(devolvidas["locacoes_devolvidas"]),
            "locacoes_atrasadas": int(atrasadas["locacoes_atrasadas"]),
        }
    finally:
        conn.close()


@router.get("/faturamento-serie")
def faturamento_serie(
    data_ini: Optional[str] = None,
    data_fim: Optional[str] = None,
    granularidade: str = "dia",  # "dia" ou "mes"
):
    if granularidade not in ("dia", "mes"):
        raise HTTPException(status_code=400, detail="granularidade deve ser 'dia' ou 'mes'.")

    dt_ini, dt_fim = parse_periodo(data_ini, data_fim)

    where = ""
    params = []
    if dt_ini and dt_fim:
        where = "WHERE data_locacao BETWEEN %s AND %s"
        params = [dt_ini, dt_fim]

    select_periodo = (
        "DATE_FORMAT(data_locacao, '%Y-%m-%d')"
        if granularidade == "dia"
        else "DATE_FORMAT(data_locacao, '%Y-%m')"
    )

    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"""
            SELECT
              {select_periodo} AS periodo,
              COUNT(*) AS total_locacoes,
              COALESCE(SUM(valor_total), 0) AS faturamento_total
            FROM locacoes
            {where}
            GROUP BY periodo
            ORDER BY periodo
            """,
            tuple(params),
        )
        rows = cur.fetchall()

        for r in rows:
            r["total_locacoes"] = int(r["total_locacoes"])
            r["faturamento_total"] = float(r["faturamento_total"])

        return rows
    finally:
        conn.close()


@router.get("/faturamento-por-genero")
def faturamento_por_genero(
    data_ini: Optional[str] = None,
    data_fim: Optional[str] = None,
):
    dt_ini, dt_fim = parse_periodo(data_ini, data_fim)

    where = ""
    params = []
    if dt_ini and dt_fim:
        where = "WHERE l.data_locacao BETWEEN %s AND %s"
        params = [dt_ini, dt_fim]

    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"""
            SELECT
              f.genero_filme AS genero,
              COUNT(*) AS total_locacoes,
              COALESCE(SUM(l.valor_total), 0) AS faturamento_total
            FROM locacoes l
            JOIN filmes f ON f.id = l.id_filme
            {where}
            GROUP BY f.genero_filme
            ORDER BY faturamento_total DESC
            """,
            tuple(params),
        )
        rows = cur.fetchall()

        for r in rows:
            r["total_locacoes"] = int(r["total_locacoes"])
            r["faturamento_total"] = float(r["faturamento_total"])

        return rows
    finally:
        conn.close()