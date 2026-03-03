from fastapi import APIRouter, HTTPException
from typing import Optional
from datetime import date

from etl.db import get_conn
from api.schemas import UsuarioCreate, UsuarioUpdate

router = APIRouter()


@router.get("/usuarios/inativos")
def listar_usuarios_inativos(limit: int = 50, offset: int = 0):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT id, nome_usuario, idade, cidade, estado, ativo
            FROM usuarios
            WHERE ativo = 0
            ORDER BY id
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        return cur.fetchall()
    finally:
        conn.close()


@router.get("/usuarios")
def listar_usuarios(incluir_inativos: bool = False, limit: int = 50, offset: int = 0):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)

        where = "" if incluir_inativos else "WHERE ativo = 1"

        cur.execute(
            f"""
            SELECT id, nome_usuario, idade, cidade, estado, ativo
            FROM usuarios
            {where}
            ORDER BY id
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        return cur.fetchall()
    finally:
        conn.close()


@router.get("/usuarios/busca")
def buscar_usuarios(
    nome: Optional[str] = None,
    cidade: Optional[str] = None,
    estado: Optional[str] = None,
    incluir_inativos: bool = False,
    limit: int = 50,
    offset: int = 0,
):
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


@router.get("/usuarios/top-gasto")
def usuarios_top_gasto(limit: int = 10, incluir_inativos: bool = False):
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


@router.get("/usuarios/{id_usuario}/gasto-total")
def gasto_total_usuario(id_usuario: int):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)

        # 1) Verificar se usuário existe
        cur.execute(
            "SELECT id, nome_usuario, ativo FROM usuarios WHERE id = %s",
            (id_usuario,),
        )
        usuario = cur.fetchone()
        if not usuario:
            raise HTTPException(status_code=404, detail="Usuário não encontrado.")

        # 2) Resumo financeiro e contagens
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

        resumo = cur.fetchone()

        return {
            "usuario": {
                "id": usuario["id"],
                "nome_usuario": usuario["nome_usuario"],
                "ativo": bool(usuario["ativo"]),
            },
            "total_locacoes": int(resumo["total_locacoes"]),
            "gasto_total": float(resumo["gasto_total"]),
            "locacoes_abertas": int(resumo["locacoes_abertas"] or 0),
            "locacoes_devolvidas": int(resumo["locacoes_devolvidas"] or 0),
            "locacoes_atrasadas": int(resumo["locacoes_atrasadas"] or 0),
        }
    finally:
        conn.close()


@router.get("/usuarios/{id_usuario}/historico-locacoes")
def historico_locacoes_usuario(id_usuario: int, limit: int = 50, offset: int = 0):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)

        # 1) Confirma se o usuário existe (e pega ativo)
        cur.execute(
            """
            SELECT id, nome_usuario, ativo
            FROM usuarios
            WHERE id = %s
            """,
            (id_usuario,),
        )
        usuario = cur.fetchone()
        if not usuario:
            raise HTTPException(status_code=404, detail="Usuário não encontrado.")

        # 2) Busca as locações do usuário (join com filmes para pegar nome_filme)
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
        locacoes = cur.fetchall()

        return {
            "usuario": {
                "id": usuario["id"],
                "nome_usuario": usuario["nome_usuario"],
                "ativo": bool(usuario["ativo"]),
            },
            "paginacao": {"limit": limit, "offset": offset},
            "total_retornado": len(locacoes),
            "locacoes": locacoes,
        }
    finally:
        conn.close()


@router.get("/usuarios/{id_usuario}")
def buscar_usuario(id_usuario: int):
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
        usuario = cur.fetchone()
        if not usuario:
            raise HTTPException(status_code=404, detail="Usuário não encontrado.")
        return usuario
    finally:
        conn.close()


@router.post("/usuarios")
def criar_usuario(usuario: UsuarioCreate):
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
        return {"message": "Usuário criado com sucesso", "id": cur.lastrowid}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()


@router.patch("/usuarios/{id_usuario}")
def atualizar_usuario(id_usuario: int, usuario: UsuarioUpdate):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)

        # 1) Verifica se existe
        cur.execute("SELECT id FROM usuarios WHERE id = %s", (id_usuario,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Usuário não encontrado.")

        # 2) Monta update só com campos enviados
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
            raise HTTPException(status_code=400, detail="Nenhum campo enviado para atualizar.")

        sql = f"UPDATE usuarios SET {', '.join(campos)} WHERE id = %s"
        valores.append(id_usuario)

        cur2 = conn.cursor()
        cur2.execute(sql, tuple(valores))
        conn.commit()

        return {"message": "Usuário atualizado com sucesso", "id_usuario": id_usuario}

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()


@router.delete("/usuarios/{id_usuario}")
def inativar_usuario(id_usuario: int):
    conn = get_conn()
    try:
        cur = conn.cursor()

        # Confirma se existe
        cur.execute("SELECT id FROM usuarios WHERE id = %s", (id_usuario,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Usuário não encontrado.")

        cur.execute("UPDATE usuarios SET ativo = 0 WHERE id = %s", (id_usuario,))
        conn.commit()
        return {"message": "Usuário inativado com sucesso", "id_usuario": id_usuario}

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()


@router.patch("/usuarios/{id_usuario}/ativar")
def ativar_usuario(id_usuario: int):
    conn = get_conn()
    try:
        cur = conn.cursor()

        cur.execute("SELECT id FROM usuarios WHERE id = %s", (id_usuario,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Usuário não encontrado.")

        cur.execute("UPDATE usuarios SET ativo = 1 WHERE id = %s", (id_usuario,))
        conn.commit()
        return {"message": "Usuário ativado com sucesso", "id_usuario": id_usuario}

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()