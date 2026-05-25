# Videolocadora – Backend em Python (FastAPI + MySQL)

Projeto completo de backend desenvolvido em **Python**, utilizando **FastAPI** e **MySQL**, simulando o sistema de uma videolocadora com:

- Controle de filmes
- Controle de usuários
- Gestão de locações
- Dashboard analítico
- Processo de ETL para carga inicial de dados
- Exportação de relatórios CSV

Este projeto foi desenvolvido como prática de:

- Modelagem relacional
- Integração Python + MySQL
- Construção de API REST
- Organização de arquitetura modular
- Queries analíticas com agregações
- ETL automatizado
- Testes automatizados
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

## Testes

- Pytest

## Ferramentas

- Git & GitHub
- DBeaver

---

# Estrutura do Projeto

```text
videolocadora-python/
│
├── api/
│   ├── main.py
│   ├── schemas.py
│   │
│   ├── routes/
│   │   ├── filmes.py
│   │   ├── usuarios.py
│   │   ├── locacoes.py
│   │   ├── dashboard.py
│   │   └── views.py
│   │
│   ├── services/
│   │   ├── filmes_service.py
│   │   ├── usuarios_service.py
│   │   ├── locacoes_service.py
│   │   ├── dashboard_service.py
│   │   └── views_service.py
│   │
│   └── repositories/
│       ├── filmes_repository.py
│       ├── usuarios_repository.py
│       ├── locacoes_repository.py
│       ├── dashboard_repository.py
│       └── views_repository.py
│
├── etl/
│   ├── db.py
│   ├── load_filmes.py
│   ├── load_usuarios.py
│   ├── load_locacoes.py
│   ├── export_relatorios.py
│   └── validate.py
│
├── tests/
│   ├── conftest.py
│   ├── test_health.py
│   ├── test_filmes.py
│   ├── test_usuarios.py
│   └── test_locacoes.py
│
├── data/
│
├── requirements.txt
└── README.md
```

Arquitetura modular com separação por domínio, permitindo escalabilidade e manutenção facilitada.

---

# Arquitetura da API

A aplicação foi refatorada utilizando separação de responsabilidades em camadas:

- **Routes:** recebem as requisições HTTP
- **Services:** concentram regras de negócio
- **Repositories:** realizam acesso ao banco de dados

Essa arquitetura reduz acoplamento, melhora manutenção e facilita escalabilidade da aplicação.

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

O projeto possui rotinas ETL desenvolvidas em Python utilizando Pandas para:

- Leitura de arquivos CSV
- Limpeza e padronização de dados
- Conversão de tipos
- Validação de colunas obrigatórias
- Validação de integridade referencial (FK)
- Prevenção de registros duplicados
- Carga incremental de dados
- Exportação de relatórios CSV

Isso permite simular carga inicial de dados no sistema.

---

# Exportação de Relatórios

O sistema também possui exportação automatizada de relatórios CSV a partir do banco de dados.

Relatórios disponíveis:

- Ranking de filmes
- Faturamento mensal
- Relatório de locações
- Exportação de usuários
- Exportação de filmes

Execução:

```bash
python3 -m etl.export_relatorios
```

---

# Testes Automatizados

O projeto utiliza Pytest para validação automatizada dos endpoints da API.

Cobertura atual:

- Filmes
- Usuários
- Locações
- Health-check

Para executar os testes:

```bash
pytest
```

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

```text
http://127.0.0.1:8000/docs
```

---

# Conceitos Aplicados

- API REST
- Arquitetura modular com APIRouter
- Arquitetura em camadas (routes/services/repositories)
- Separação de responsabilidades
- Tratamento de erros HTTP
- Queries com agregações e filtros dinâmicos
- ETL automatizado
- ETL incremental
- Integridade referencial com FK
- Testes automatizados com Pytest
- Versionamento semântico

---

# Próximas Evoluções

- Implementação de autenticação JWT
- Dockerização
- Deploy em ambiente cloud
- Frontend consumindo a API
- CI/CD com GitHub Actions

---

# Autora

**Daiane Cristina**  
Backend Developer (em transição de carreira)

GitHub: https://github.com/daiane-dev
