import pandas as pd
from etl.db import get_conn


def exportar_csv(nome_arquivo, query):
    conn = get_conn()

    try:
        df = pd.read_sql(query, conn)

        caminho = f"data/{nome_arquivo}"

        df.to_csv(
            caminho,
            index=False,
            encoding="utf-8-sig"
        )

        print(f"OK: relatório exportado -> {caminho}")

    finally:
        conn.close()


def exportar_filmes_ranking():
    query = """
        SELECT
            id_filme,
            nome_filme,
            total_locacoes,
            faturamento_total
        FROM vw_filmes_ranking
        ORDER BY faturamento_total DESC
    """

    exportar_csv(
        "relatorio_filmes_ranking.csv",
        query
    )


def exportar_faturamento_mensal():
    query = """
        SELECT
            mes,
            total_locacoes,
            faturamento_mes
        FROM vw_faturamento_mensal
        ORDER BY mes
    """

    exportar_csv(
        "relatorio_faturamento_mensal.csv",
        query
    )


def exportar_locacoes():
    query = """
        SELECT
            l.id_locacao,
            u.nome_usuario,
            f.nome_filme,
            l.data_locacao,
            l.status_locacao,
            l.valor_total
        FROM locacoes l
        JOIN usuarios u
            ON u.id = l.id_usuario
        JOIN filmes f
            ON f.id = l.id_filme
        ORDER BY l.id_locacao DESC
    """

    exportar_csv(
        "relatorio_locacoes.csv",
        query
    )


def exportar_usuarios():
    query = """
        SELECT
            id,
            nome_usuario,
            idade,
            cidade,
            estado,
            ativo
        FROM usuarios
        ORDER BY id
    """

    exportar_csv("usuarios_export.csv", query)


def exportar_filmes():
    query = """
        SELECT
            id,
            nome_filme,
            genero_filme,
            ano_filme,
            ativo
        FROM filmes
        ORDER BY id
    """

    exportar_csv("filmes_export.csv", query)


def exportar_locacoes_completas():
    query = """
        SELECT
            l.id_locacao,
            l.id_usuario,
            u.nome_usuario AS usuario,
            l.id_filme,
            f.nome_filme AS filme,
            l.data_locacao,
            l.data_prevista_locacao,
            l.data_devolucao,
            l.valor_diaria,
            l.dias,
            l.valor_total,
            l.status_locacao
        FROM locacoes l
        JOIN usuarios u
            ON u.id = l.id_usuario
        JOIN filmes f
            ON f.id = l.id_filme
        ORDER BY l.id_locacao
    """

    exportar_csv("locacoes_export.csv", query)    


if __name__ == "__main__":
    exportar_filmes_ranking()
    exportar_faturamento_mensal()
    exportar_locacoes()

    exportar_usuarios()
    exportar_filmes()
    exportar_locacoes_completas()    