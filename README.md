# Videolocadora – Backend em Python (FastAPI + MySQL)

Projeto completo de backend desenvolvido em **Python**, utilizando **FastAPI** e **MySQL**, simulando o sistema de uma videolocadora com:

- Controle de filmes
- Controle de usuários
- Gestão de locações
- Dashboard analítico
- Processo de ETL para carga inicial de dados

Este projeto foi desenvolvido como prática de:

- Modelagem relacional
- Integração Python + MySQL
- Construção de API REST
- Organização de arquitetura modular
- Queries analíticas com agregações
- Versionamento profissional com Git

---

# Tecnologias Utilizadas

## Backend
- Python 3.9+
- FastAPI
- Pydantic
- Uvicorn

## Banco de Dados
- MySQL
- Views analíticas
- Agregações SQL

## ETL
- pandas
- mysql-connector-python
- python-dotenv

## Ferramentas
- Git & GitHub

---

# Estrutura do Projeto

```
videolocadora-python/
│
├── api/
│   ├── main.py
│   ├── schemas.py
│   └── routes/
│       ├── filmes.py
│       ├── usuarios.py
│       ├── locacoes.py
│       ├── dashboard.py
│       └── views.py
│
├── etl/
│   ├── db.py
│   ├── load_filmes.py
│   ├── load_usuarios.py
│   ├── load_locacoes.py
│   └── validate.py
│
├── data/
│   ├── filmes.csv
│   ├── usuarios.csv
│   └── locacoes.csv
│
├── requirements.txt
└── README.md
```

Arquitetura modular com separação por domínio, permitindo escalabilidade e manutenção facilitada.

---

# Funcionalidades da API

## Filmes
- Criar filme
- Atualizar filme
- Inativar / Ativar
- Buscar por filtros
- Listar filmes mais locados
- Verificar disponibilidade
- Ranking via view

## Usuários
- CRUD completo
- Histórico de locações
- Gasto total por usuário
- Ranking de usuários por gasto

## Locações
- Criar locação
- Devolver locação
- Listar abertas
- Listar atrasadas
- Locações por usuário
- Locações por filme

## Dashboard
- Resumo geral
- Faturamento por período
- Série temporal (dia/mês)
- Faturamento por gênero

## Views
- Ranking de filmes
- Faturamento mensal

---

# Processo de ETL

O projeto também inclui um pipeline de ETL responsável por:

- Leitura de arquivos CSV
- Validação de dados
- Limpeza de inconsistências
- Carga automatizada no MySQL
- Relatórios de validação

Isso permite simular carga inicial de dados no sistema.

---

# Como rodar o projeto

## Clonar o repositório

```bash
git clone https://github.com/daiane-dev/videolocadora-python.git
cd videolocadora-python
```

## Criar e ativar ambiente virtual

```bash
python3 -m venv .venv
source .venv/bin/activate
```

## Instalar dependências

```bash
pip install -r requirements.txt
```

## Configurar variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto com o seguinte conteúdo:

```env
DB_HOST=localhost
DB_USER=seu_usuario
DB_PASSWORD=sua_senha
DB_NAME=videolocadora
```

## Rodar a API

```bash
uvicorn api.main:app --reload
```

Acesse a documentação interativa da API em:

```
http://127.0.0.1:8000/docs
```

---

# Conceitos Aplicados

- API REST
- Arquitetura modular com APIRouter
- Separação de responsabilidades
- Tratamento de erros HTTP
- Queries com agregações e filtros dinâmicos
- ETL automatizado
- Versionamento semântico

---

# Próximas Evoluções

- Implementação de autenticação JWT
- Testes automatizados com Pytest
- Dockerização
- Deploy em ambiente cloud
- Frontend consumindo a API

---

#  Autora

**Daiane Cristina**  
Backend Developer (em transição de carreira)  
GitHub: https://github.com/daiane-dev