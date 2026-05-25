from etl.db import get_conn


def listar_filmes(limit=50, offset=0):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT id, nome_filme, genero_filme, ano_filme
            FROM filmes
            WHERE ativo = 1
            ORDER BY id
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        return cur.fetchall()
    finally:
        conn.close()


def inserir_filme(filme):
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
        return cur.lastrowid
    finally:
        conn.close()


def filmes_mais_locados(limit=10, incluir_inativos=False):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)

        where_sql = ""
        if not incluir_inativos:
            where_sql = "WHERE f.ativo = 1"

        sql = f"""
            SELECT
                f.id,
                f.nome_filme,
                f.genero_filme,
                f.ano_filme,
                f.ativo,
                COUNT(l.id_locacao) AS total_locacoes
            FROM filmes f
            LEFT JOIN locacoes l ON l.id_filme = f.id
            {where_sql}
            GROUP BY f.id, f.nome_filme, f.genero_filme, f.ano_filme, f.ativo
            ORDER BY total_locacoes DESC, f.id ASC
            LIMIT %s
        """

        cur.execute(sql, (limit,))
        return cur.fetchall()
    finally:
        conn.close()


def listar_filmes_inativos(limit=50, offset=0):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT id, nome_filme, genero_filme, ano_filme
            FROM filmes
            WHERE ativo = 0
            ORDER BY id
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        return cur.fetchall()
    finally:
        conn.close()


def buscar_filme_por_id(id_filme):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT id, nome_filme, genero_filme, ano_filme, ativo
            FROM filmes
            WHERE id = %s
            """,
            (id_filme,),
        )
        return cur.fetchone()
    finally:
        conn.close()


def inativar_filme_por_id(id_filme):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE filmes
            SET ativo = 0
            WHERE id = %s
            """,
            (id_filme,),
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def buscar_filmes(
    nome=None,
    genero=None,
    ano_min=None,
    ano_max=None,
    incluir_inativos=False,
    limit=50,
    offset=0,
):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)

        where = []
        params = []

        if not incluir_inativos:
            where.append("ativo = 1")

        if nome:
            where.append("nome_filme LIKE %s")
            params.append(f"%{nome}%")

        if genero:
            where.append("genero_filme = %s")
            params.append(genero)

        if ano_min is not None:
            where.append("ano_filme >= %s")
            params.append(ano_min)

        if ano_max is not None:
            where.append("ano_filme <= %s")
            params.append(ano_max)

        where_sql = " WHERE " + " AND ".join(where) if where else ""

        sql = f"""
            SELECT id, nome_filme, genero_filme, ano_filme, ativo
            FROM filmes
            {where_sql}
            ORDER BY nome_filme
            LIMIT %s OFFSET %s
        """

        params.extend([limit, offset])

        cur.execute(sql, tuple(params))
        return cur.fetchall()
    finally:
        conn.close()


def buscar_filme_para_disponibilidade(id_filme):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT id, nome_filme, ativo
            FROM filmes
            WHERE id = %s
            """,
            (id_filme,),
        )
        return cur.fetchone()
    finally:
        conn.close()


def contar_locacoes_abertas_por_filme(id_filme):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT COUNT(*) AS qtd
            FROM locacoes
            WHERE id_filme = %s AND status_locacao = 'ABERTA'
            """,
            (id_filme,),
        )
        resultado = cur.fetchone()
        return resultado["qtd"]
    finally:
        conn.close()


def obter_filme_por_id(id_filme):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT id, nome_filme, genero_filme, ano_filme, ativo
            FROM filmes
            WHERE id = %s
            """,
            (id_filme,),
        )
        return cur.fetchone()
    finally:
        conn.close()


def atualizar_filme_por_id(id_filme, filme):
    data = {k: v for k, v in filme.dict().items() if v is not None}

    if not data:
        return None, None

    fields = ", ".join([f"{k}=%s" for k in data.keys()])
    values = list(data.values())

    conn = get_conn()
    try:
        cur = conn.cursor()

        cur.execute("SELECT id FROM filmes WHERE id = %s", (id_filme,))
        if cur.fetchone() is None:
            return None, None

        cur.execute(
            f"UPDATE filmes SET {fields} WHERE id = %s",
            (*values, id_filme),
        )
        conn.commit()
        return cur.rowcount, list(data.keys())
    finally:
        conn.close()


def ativar_filme_por_id(id_filme):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE filmes
            SET ativo = 1
            WHERE id = %s
            """,
            (id_filme,),
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()                                                        