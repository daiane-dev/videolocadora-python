from fastapi import APIRouter, HTTPException
from typing import Optional
from etl.db import get_conn
from api.schemas import FilmeCreate, FilmeUpdate

router = APIRouter()


@router.get("/filmes")
def listar_filmes(limit: int = 50, offset: int = 0):
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


@router.post("/filmes")
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


@router.get("/filmes/mais-locados")
def filmes_mais_locados(limit: int = 10, incluir_inativos: bool = False):
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


@router.get("/filmes/inativos")
def listar_filmes_inativos(limit: int = 50, offset: int = 0):
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


@router.delete("/filmes/{id_filme}")
def deletar_filme(id_filme: int):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE filmes SET ativo = 0 WHERE id = %s", (id_filme,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Filme não encontrado")
        conn.commit()
        return {"message": "Filme inativado com sucesso", "id": id_filme}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()


@router.get("/filmes/busca")
def buscar_filmes(
    nome: Optional[str] = None,
    genero: Optional[str] = None,
    ano_min: Optional[int] = None,
    ano_max: Optional[int] = None,
    incluir_inativos: bool = False,
    limit: int = 50,
    offset: int = 0,
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


@router.get("/filmes/{id_filme}/disponibilidade")
def disponibilidade_filme(id_filme: int):
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
        filme = cur.fetchone()
        if not filme:
            raise HTTPException(status_code=404, detail="Filme não encontrado.")

        cur.execute(
            """
            SELECT COUNT(*) AS qtd
            FROM locacoes
            WHERE id_filme = %s AND status_locacao = 'ABERTA'
            """,
            (id_filme,),
        )
        qtd_abertas = cur.fetchone()["qtd"]

        tem_locacao_aberta = qtd_abertas > 0
        ativo = filme["ativo"] == 1
        disponivel = ativo and (not tem_locacao_aberta)

        return {
            "id_filme": filme["id"],
            "nome_filme": filme["nome_filme"],
            "ativo": ativo,
            "tem_locacao_aberta": tem_locacao_aberta,
            "disponivel": disponivel,
        }
    finally:
        conn.close()


@router.get("/filmes/{id_filme}")
def obter_filme(id_filme: int):
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
        filme = cur.fetchone()
        if filme is None:
            raise HTTPException(status_code=404, detail="Filme não encontrado")
        return filme
    finally:
        conn.close()


@router.patch("/filmes/{id_filme}")
def atualizar_filme(id_filme: int, filme: FilmeUpdate):
    data = {k: v for k, v in filme.dict().items() if v is not None}
    if not data:
        raise HTTPException(status_code=400, detail="Envie ao menos 1 campo para atualizar")

    fields = ", ".join([f"{k}=%s" for k in data.keys()])
    values = list(data.values())

    conn = get_conn()
    try:
        cur = conn.cursor()

        cur.execute("SELECT id FROM filmes WHERE id = %s", (id_filme,))
        if cur.fetchone() is None:
            raise HTTPException(status_code=404, detail="Filme não encontrado")

        cur.execute(
            f"UPDATE filmes SET {fields} WHERE id = %s",
            (*values, id_filme),
        )
        conn.commit()
        return {
            "message": "Filme atualizado com sucesso",
            "id": id_filme,
            "updated_fields": list(data.keys()),
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()


@router.patch("/filmes/{id_filme}/ativar")
def ativar_filme(id_filme: int):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE filmes SET ativo = 1 WHERE id = %s", (id_filme,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Filme não encontrado")
        conn.commit()
        return {"message": "Filme reativado com sucesso", "id": id_filme}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()


