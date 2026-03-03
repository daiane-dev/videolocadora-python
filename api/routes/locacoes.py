from fastapi import APIRouter, HTTPException
from typing import Optional
from datetime import date
from etl.db import get_conn


router = APIRouter(prefix="/locacoes")


@router.get("/abertas")
def listar_locacoes_abertas(limit: int = 50, offset: int = 0):
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


@router.get("/atrasadas")
def listar_locacoes_atrasadas(limit: int = 50, offset: int = 0):
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

        rows = cur.fetchall()
        hoje = date.today()
        for r in rows:
            prevista = r["data_prevista_locacao"]
            r["dias_atraso"] = (hoje - prevista).days if prevista else None

        return rows
    finally:
        conn.close()


@router.get("/por-usuario/{id_usuario}")
def listar_locacoes_por_usuario(id_usuario: int, limit: int = 50, offset: int = 0):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)

        cur.execute("SELECT 1 FROM usuarios WHERE id = %s", (id_usuario,))
        if cur.fetchone() is None:
            raise HTTPException(status_code=404, detail="Usuário não encontrado")

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


@router.get("/por-filme/{id_filme}")
def listar_locacoes_por_filme(id_filme: int, limit: int = 50, offset: int = 0):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)

        cur.execute("SELECT id FROM filmes WHERE id = %s", (id_filme,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Filme não encontrado.")

        cur.execute(
            """
            SELECT
              id_locacao,
              id_usuario,
              nome_usuario,
              id_filme,
              nome_filme,
              data_locacao,
              data_prevista_locacao,
              data_devolucao,
              valor_diaria,
              dias,
              valor_total,
              status_locacao
            FROM vw_locacoes_detalhadas
            WHERE id_filme = %s
            ORDER BY data_locacao DESC, id_locacao DESC
            LIMIT %s OFFSET %s
            """,
            (id_filme, limit, offset),
        )
        return cur.fetchall()
    finally:
        conn.close()


@router.get("/{id_locacao}")
def buscar_locacao(id_locacao: int):
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
            WHERE l.id_locacao = %s
            """,
            (id_locacao,),
        )
        locacao = cur.fetchone()
        if not locacao:
            raise HTTPException(status_code=404, detail="Locação não encontrada.")
        return locacao
    finally:
        conn.close()


@router.patch("/{id_locacao}/devolver")
def devolver_locacao(id_locacao: int, data_devolucao: Optional[str] = None):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)

        cur.execute(
            "SELECT id_locacao, status_locacao FROM locacoes WHERE id_locacao = %s",
            (id_locacao,),
        )
        loc = cur.fetchone()
        if not loc:
            raise HTTPException(status_code=404, detail="Locação não encontrada")

        if loc["status_locacao"] == "DEVOLVIDA":
            return {"message": "Locação já estava devolvida", "id_locacao": id_locacao}

        dt = data_devolucao or date.today().isoformat()

        cur.execute(
            """
            UPDATE locacoes
            SET data_devolucao = %s,
                status_locacao = 'DEVOLVIDA'
            WHERE id_locacao = %s
            """,
            (dt, id_locacao),
        )

        conn.commit()
        return {"message": "Locação devolvida com sucesso", "id_locacao": id_locacao, "data_devolucao": dt}

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()