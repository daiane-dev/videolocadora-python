from api.repositories import usuarios_repository
from fastapi import HTTPException


def historico_locacoes_usuario(id_usuario, limit=50, offset=0):
    usuario = usuarios_repository.buscar_usuario_por_id(id_usuario)

    if not usuario:
        raise HTTPException(status_code=404, detail="Usuário não encontrado.")

    locacoes = usuarios_repository.obter_historico_locacoes_usuario(
        id_usuario=id_usuario,
        limit=limit,
        offset=offset,
    )

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

def buscar_usuarios(nome=None, cidade=None, estado=None, incluir_inativos=False, limit=50, offset=0):
    return usuarios_repository.buscar_usuarios(
        nome=nome,
        cidade=cidade,
        estado=estado,
        incluir_inativos=incluir_inativos,
        limit=limit,
        offset=offset,
    )   


def obter_usuario(id_usuario):
    usuario = usuarios_repository.buscar_usuario_por_id(id_usuario)

    if not usuario:
        raise HTTPException(status_code=404, detail="Usuário não encontrado.")

    return usuario


def criar_usuario(usuario):
    usuario_id = usuarios_repository.inserir_usuario(usuario)

    return {
        "message": "Usuário criado com sucesso",
        "id_usuario": usuario_id
    }


def listar_usuarios():
    return usuarios_repository.listar_usuarios_ativos()


def listar_usuarios_inativos():
    return usuarios_repository.listar_usuarios_inativos()   


def criar_usuario(usuario):
    usuario_id = usuarios_repository.inserir_usuario(usuario)
    return {
        "message": "Usuário criado com sucesso",
        "id_usuario": usuario_id
    }


def atualizar_usuario(id_usuario, usuario):
    usuario_existente = usuarios_repository.buscar_usuario_por_id(id_usuario)

    if not usuario_existente:
        raise HTTPException(status_code=404, detail="Usuário não encontrado.")

    if (
        usuario.nome_usuario is None
        and usuario.idade is None
        and usuario.cidade is None
        and usuario.estado is None
    ):
        raise HTTPException(status_code=400, detail="Nenhum campo enviado para atualizar.")

    usuarios_repository.atualizar_usuario_por_id(id_usuario, usuario)

    return {
        "message": "Usuário atualizado com sucesso",
        "id_usuario": id_usuario
    }


def inativar_usuario(id_usuario):
    usuario = usuarios_repository.buscar_usuario_por_id(id_usuario)

    if not usuario:
        raise HTTPException(status_code=404, detail="Usuário não encontrado.")

    usuarios_repository.inativar_usuario_por_id(id_usuario)

    return {
        "message": "Usuário inativado com sucesso",
        "id_usuario": id_usuario
    }


def ativar_usuario(id_usuario):
    usuario = usuarios_repository.buscar_usuario_por_id(id_usuario)

    if not usuario:
        raise HTTPException(status_code=404, detail="Usuário não encontrado.")

    usuarios_repository.ativar_usuario_por_id(id_usuario)

    return {
        "message": "Usuário ativado com sucesso",
        "id_usuario": id_usuario
    }    


def usuarios_top_gasto(limit=10, incluir_inativos=False):
    return usuarios_repository.usuarios_top_gasto(
        limit=limit,
        incluir_inativos=incluir_inativos,
    )


def gasto_total_usuario(id_usuario):
    usuario = usuarios_repository.buscar_usuario_por_id(id_usuario)

    if not usuario:
        raise HTTPException(status_code=404, detail="Usuário não encontrado.")

    resumo = usuarios_repository.obter_gasto_total_usuario(id_usuario)

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