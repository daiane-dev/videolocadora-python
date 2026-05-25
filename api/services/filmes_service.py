from api.repositories import filmes_repository
from fastapi import HTTPException


def listar_filmes(limit=50, offset=0):
    return filmes_repository.listar_filmes(limit=limit, offset=offset)


def criar_filme(filme):
    try:
        filme_id = filmes_repository.inserir_filme(filme)
        return {
            "message": "Filme criado com sucesso",
            "id": filme_id
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def filmes_mais_locados(limit=10, incluir_inativos=False):
    return filmes_repository.filmes_mais_locados(
        limit=limit,
        incluir_inativos=incluir_inativos,
    )


def listar_filmes_inativos(limit=50, offset=0):
    return filmes_repository.listar_filmes_inativos(
        limit=limit,
        offset=offset,
    )


def deletar_filme(id_filme):
    filme = filmes_repository.buscar_filme_por_id(id_filme)

    if not filme:
        raise HTTPException(status_code=404, detail="Filme não encontrado")

    filmes_repository.inativar_filme_por_id(id_filme)

    return {
        "message": "Filme inativado com sucesso",
        "id": id_filme
    }


def buscar_filmes(
    nome=None,
    genero=None,
    ano_min=None,
    ano_max=None,
    incluir_inativos=False,
    limit=50,
    offset=0,
):
    return filmes_repository.buscar_filmes(
        nome=nome,
        genero=genero,
        ano_min=ano_min,
        ano_max=ano_max,
        incluir_inativos=incluir_inativos,
        limit=limit,
        offset=offset,
    )


def disponibilidade_filme(id_filme):
    filme = filmes_repository.buscar_filme_para_disponibilidade(id_filme)

    if not filme:
        raise HTTPException(status_code=404, detail="Filme não encontrado.")

    qtd_abertas = filmes_repository.contar_locacoes_abertas_por_filme(id_filme)

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


def obter_filme(id_filme):
    filme = filmes_repository.obter_filme_por_id(id_filme)

    if filme is None:
        raise HTTPException(status_code=404, detail="Filme não encontrado")

    return filme


def atualizar_filme(id_filme, filme):
    data = {k: v for k, v in filme.dict().items() if v is not None}
    if not data:
        raise HTTPException(status_code=400, detail="Envie ao menos 1 campo para atualizar")

    filme_existente = filmes_repository.obter_filme_por_id(id_filme)
    if filme_existente is None:
        raise HTTPException(status_code=404, detail="Filme não encontrado")

    _, updated_fields = filmes_repository.atualizar_filme_por_id(id_filme, filme)

    return {
        "message": "Filme atualizado com sucesso",
        "id": id_filme,
        "updated_fields": updated_fields,
    }


def ativar_filme(id_filme):
    filme = filmes_repository.obter_filme_por_id(id_filme)

    if filme is None:
        raise HTTPException(status_code=404, detail="Filme não encontrado")

    filmes_repository.ativar_filme_por_id(id_filme)

    return {
        "message": "Filme reativado com sucesso",
        "id": id_filme
    }                                        