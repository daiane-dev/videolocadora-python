from etl.db import get_conn


def listar_locacoes_abertas(limit=50, offset=0):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT
                l.id_locacao,
                u.nome_usuario,
                f.nome_filme,
                l.data_locacao,
                l.data_prevista_locacao,
                l.valor_diaria,
                l.dias,
                l.valor_total,
                l.status_locacao
            FROM locacoes l
            JOIN usuarios u ON u.id = l.id_usuario
            JOIN filmes f ON f.id = l.id_filme
            WHERE l.status_locacao = 'ABERTA'
            ORDER BY l.data_prevista_locacao
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        return cur.fetchall()
    finally:
        conn.close()


def listar_locacoes_atrasadas(limit=50, offset=0):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT
              l.id_locacao,
              u.nome_usuario,
              f.nome_filme,
              l.data_locacao,
              l.data_prevista_locacao,
              l.valor_diaria,
              l.dias,
              l.valor_total,
              l.status_locacao
            FROM locacoes l
            JOIN usuarios u ON u.id = l.id_usuario
            JOIN filmes f ON f.id = l.id_filme
            WHERE l.status_locacao = 'ABERTA'
              AND l.data_prevista_locacao < CURDATE()
            ORDER BY l.data_prevista_locacao ASC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        return cur.fetchall()
    finally:
        conn.close()


def listar_locacoes_por_usuario(id_usuario, limit=50, offset=0):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT
              l.id_locacao,
              l.id_usuario,
              u.nome_usuario,
              l.id_filme,
              f.nome_filme,
              l.data_locacao,
              l.data_prevista_locacao,
              l.data_devolucao,
              l.valor_diaria,
              l.dias,
              l.valor_total,
              l.status_locacao
            FROM locacoes l
            JOIN usuarios u ON u.id = l.id_usuario
            JOIN filmes f ON f.id = l.id_filme
            WHERE l.id_usuario = %s
            ORDER BY l.data_locacao DESC, l.id_locacao DESC
            LIMIT %s OFFSET %s
            """,
            (id_usuario, limit, offset),
        )
        return cur.fetchall()
    finally:
        conn.close()                