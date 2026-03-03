from pydantic import BaseModel
from typing import Optional


class FilmeCreate(BaseModel):
    nome_filme: str
    genero_filme: str
    ano_filme: int


class FilmeUpdate(BaseModel):
    nome_filme: Optional[str] = None
    genero_filme: Optional[str] = None
    ano_filme: Optional[int] = None


class UsuarioCreate(BaseModel):
    nome_usuario: str
    idade: int
    cidade: str
    estado: str


class UsuarioUpdate(BaseModel):
    nome_usuario: Optional[str] = None
    idade: Optional[int] = None
    cidade: Optional[str] = None
    estado: Optional[str] = None


class LocacaoCreate(BaseModel):
    id_usuario: int
    id_filme: int
    data_locacao: str
    data_prevista_locacao: str
    data_devolucao: Optional[str] = None
    valor_diaria: float
    dias: int