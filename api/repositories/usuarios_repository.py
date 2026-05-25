from etl.db import get_conn

def buscar_usuarios(nome=None, cidade=None, estado=None, incluir_inativos=False, limit=50, offset=0):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)

        where = []
        params = []

        if nome:
            where.append("nome_usuario LIKE %s")
            params.append(f"%{nome}%")

        if cidade:
            where.append("cidade LIKE %s")
            params.append(f"%{cidade}%")

        if estado:
            where.append("estado = %s")
            params.append(estado)

        if not incluir_inativos:
            where.append("ativo = 1")

        where_sql = " AND ".join(where) if where else "1=1"

        sql = f"""
            SELECT id, nome_usuario, idade, cidade, estado, ativo
            FROM usuarios
            WHERE {where_sql}
            ORDER BY id
            LIMIT %s OFFSET %s
        """

        params.extend([limit, offset])
        cur.execute(sql, tuple(params))
        return cur.fetchall()
    finally:
        conn.close() 


def inserir_usuario(usuario):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO usuarios (nome_usuario, idade, cidade, estado)
            VALUES (%s, %s, %s, %s)
            """,
            (usuario.nome_usuario, usuario.idade, usuario.cidade, usuario.estado),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def listar_usuarios_ativos():
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT id, nome_usuario, idade, cidade, estado, ativo
            FROM usuarios
            WHERE ativo = 1
            ORDER BY id
            """
        )
        return cur.fetchall()
    finally:
        conn.close()


def listar_usuarios_inativos():
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT id, nome_usuario, idade, cidade, estado, ativo
            FROM usuarios
            WHERE ativo = 0
            ORDER BY id
            """
        )
        return cur.fetchall()
    finally:
        conn.close()


def inserir_usuario(usuario):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO usuarios (nome_usuario, idade, cidade, estado)
            VALUES (%s, %s, %s, %s)
            """,
            (usuario.nome_usuario, usuario.idade, usuario.cidade, usuario.estado),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def buscar_usuario_por_id(id_usuario):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT id, nome_usuario, idade, cidade, estado, ativo
            FROM usuarios
            WHERE id = %s
            """,
            (id_usuario,),
        )
        return cur.fetchone()
    finally:
        conn.close()


def inativar_usuario_por_id(id_usuario):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE usuarios
            SET ativo = 0
            WHERE id = %s
            """,
            (id_usuario,),
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def ativar_usuario_por_id(id_usuario):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE usuarios
            SET ativo = 1
            WHERE id = %s
            """,
            (id_usuario,),
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def atualizar_usuario_por_id(id_usuario, usuario):
    conn = get_conn()
    try:
        cur = conn.cursor()

        campos = []
        valores = []

        if usuario.nome_usuario is not None:
            campos.append("nome_usuario = %s")
            valores.append(usuario.nome_usuario)

        if usuario.idade is not None:
            campos.append("idade = %s")
            valores.append(usuario.idade)

        if usuario.cidade is not None:
            campos.append("cidade = %s")
            valores.append(usuario.cidade)

        if usuario.estado is not None:
            campos.append("estado = %s")
            valores.append(usuario.estado)

        if not campos:
            return 0

        sql = f"UPDATE usuarios SET {', '.join(campos)} WHERE id = %s"
        valores.append(id_usuario)

        cur.execute(sql, tuple(valores))
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def usuarios_top_gasto(limit=10, incluir_inativos=False):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)

        where_usuario = ""
        if not incluir_inativos:
            where_usuario = "WHERE u.ativo = 1"

        cur.execute(
            f"""
            SELECT
                u.id AS id_usuario,
                u.nome_usuario,
                u.ativo,
                COUNT(l.id_locacao) AS total_locacoes,
                COALESCE(SUM(l.valor_total), 0) AS gasto_total
            FROM usuarios u
            LEFT JOIN locacoes l
                ON l.id_usuario = u.id
            {where_usuario}
            GROUP BY u.id, u.nome_usuario, u.ativo
            ORDER BY gasto_total DESC, total_locacoes DESC, u.id ASC
            LIMIT %s
            """,
            (limit,),
        )

        return cur.fetchall()
    finally:
        conn.close()
       

def obter_gasto_total_usuario(id_usuario):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT
                COUNT(*) AS total_locacoes,
                COALESCE(SUM(valor_total), 0) AS gasto_total,
                SUM(CASE WHEN status_locacao = 'ABERTA' THEN 1 ELSE 0 END) AS locacoes_abertas,
                SUM(CASE WHEN status_locacao = 'DEVOLVIDA' THEN 1 ELSE 0 END) AS locacoes_devolvidas,
                SUM(CASE WHEN status_locacao = 'ABERTA' AND data_prevista_locacao < CURDATE() THEN 1 ELSE 0 END) AS locacoes_atrasadas
            FROM locacoes
            WHERE id_usuario = %s
            """,
            (id_usuario,),
        )
        return cur.fetchone()
    finally:
        conn.close()


def obter_historico_locacoes_usuario(id_usuario, limit=50, offset=0):
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
            ORDER BY l.id_locacao DESC
            LIMIT %s OFFSET %s
            """,
            (id_usuario, limit, offset),
        )
        return cur.fetchall()
    finally:
        conn.close()        