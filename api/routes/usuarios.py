from fastapi import APIRouter, HTTPException
from typing import Optional
from datetime import date

from etl.db import get_conn
from api.schemas import UsuarioCreate, UsuarioUpdate
from api.services import usuarios_service

router = APIRouter()

@router.get("/usuarios/busca")
def buscar_usuarios(
    nome: Optional[str] = None,
    cidade: Optional[str] = None,
    estado: Optional[str] = None,
    incluir_inativos: bool = False,
    limit: int = 50,
    offset: int = 0,
):
    return usuarios_service.buscar_usuarios(
        nome=nome,
        cidade=cidade,
        estado=estado,
        incluir_inativos=incluir_inativos,
        limit=limit,
        offset=offset,
    )


@router.get("/usuarios/top-gasto")
def usuarios_top_gasto(limit: int = 10, incluir_inativos: bool = False):
    return usuarios_service.usuarios_top_gasto(
        limit=limit,
        incluir_inativos=incluir_inativos,
    )

@router.get("/usuarios/{id_usuario}")
def buscar_usuario(id_usuario: int):
    return usuarios_service.obter_usuario(id_usuario)


def listar_usuarios():
    return usuarios_repository.listar_usuarios_ativos()


def listar_usuarios_inativos():
    return usuarios_repository.listar_usuarios_inativos()


@router.patch("/usuarios/{id_usuario}")
def atualizar_usuario(id_usuario: int, usuario: UsuarioUpdate):
    return usuarios_service.atualizar_usuario(id_usuario, usuario)


@router.post("/usuarios")
def criar_usuario(usuario: UsuarioCreate):
    return usuarios_service.criar_usuario(usuario)


@router.get("/usuarios")
def listar_usuarios():
    return usuarios_service.listar_usuarios()   


@router.delete("/usuarios/{id_usuario}")
def inativar_usuario(id_usuario: int):
    return usuarios_service.inativar_usuario(id_usuario)


@router.patch("/usuarios/{id_usuario}/ativar")
def ativar_usuario(id_usuario: int):
    return usuarios_service.ativar_usuario(id_usuario)


@router.get("/usuarios/{id_usuario}/gasto-total")
def gasto_total_usuario(id_usuario: int):
    return usuarios_service.gasto_total_usuario(id_usuario)


@router.get("/usuarios/{id_usuario}/historico-locacoes")
def historico_locacoes_usuario(id_usuario: int, limit: int = 50, offset: int = 0):
    return usuarios_service.historico_locacoes_usuario(
        id_usuario=id_usuario,
        limit=limit,
        offset=offset,
    )