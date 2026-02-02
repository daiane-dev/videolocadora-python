# Videolocadora – ETL em Python com MySQL

Projeto de **ETL (Extract, Transform, Load)** desenvolvido em Python para integração com banco de dados MySQL, simulando o funcionamento de uma videolocadora.

O objetivo do projeto é praticar:

- Integração Python + MySQL
- Inserção de dados em lote via CSV
- Validação e limpeza de dados antes da carga
- Automação de carga de dados
- Criação de relatórios de validação
- Organização de projeto para portfólio profissional

Este projeto complementa o repositório SQL do sistema, onde estão definidas as tabelas e views analíticas.

---

## Tecnologias utilizadas

- Python 3
- MySQL
- pandas
- mysql-connector-python
- python-dotenv
- Git & GitHub

---

## Estrutura do projeto

```
videolocadora-python/
├── data/
│ ├── filmes.csv
│ ├── usuarios.csv
│ └── locacoes.csv
│
├── etl/
│ ├── db.py
│ ├── load_filmes.py
│ ├── load_usuarios.py
│ ├── load_locacoes.py
│ └── validate.py
│
├── test_db.py
├── requirements.txt
└── .gitignore
```
