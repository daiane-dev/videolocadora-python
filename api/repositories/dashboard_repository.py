from etl.db import get_conn


def obter_resumo_dashboard(hoje):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)

        cur.execute("SELECT COUNT(*) AS total_usuarios FROM usuarios")
        total_usuarios = int(cur.fetchone()["total_usuarios"])

        cur.execute("SELECT COUNT(*) AS total_filmes_ativos FROM filmes WHERE ativo = 1")
        total_filmes_ativos = int(cur.fetchone()["total_filmes_ativos"])

        cur.execute("SELECT COUNT(*) AS total_filmes_inativos FROM filmes WHERE ativo = 0")
        total_filmes_inativos = int(cur.fetchone()["total_filmes_inativos"])

        cur.execute("SELECT COUNT(*) AS total_locacoes FROM locacoes")
        total_locacoes = int(cur.fetchone()["total_locacoes"])

        cur.execute("SELECT COUNT(*) AS locacoes_abertas FROM locacoes WHERE status_locacao = 'ABERTA'")
        locacoes_abertas = int(cur.fetchone()["locacoes_abertas"])

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

        cur.execute("SELECT COALESCE(SUM(valor_total), 0) AS faturamento_total FROM locacoes")
        faturamento_total = float(cur.fetchone()["faturamento_total"])

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


def obter_faturamento_por_periodo(dt_ini=None, dt_fim=None, hoje=None):
    where = ""
    params = []

    if dt_ini and dt_fim:
        where = "WHERE data_locacao BETWEEN %s AND %s"
        params = [dt_ini, dt_fim]

    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)

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
            "total_locacoes": int(resumo["total_locacoes"]),
            "faturamento_total": float(resumo["faturamento_total"]),
            "locacoes_abertas": int(abertas["locacoes_abertas"]),
            "locacoes_devolvidas": int(devolvidas["locacoes_devolvidas"]),
            "locacoes_atrasadas": int(atrasadas["locacoes_atrasadas"]),
        }

    finally:
        conn.close()    


def obter_faturamento_serie(dt_ini=None, dt_fim=None, granularidade="dia"):
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
        return cur.fetchall()
    finally:
        conn.close()


def obter_faturamento_por_genero(dt_ini=None, dt_fim=None):
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
        return cur.fetchall()
    finally:
        conn.close()                        