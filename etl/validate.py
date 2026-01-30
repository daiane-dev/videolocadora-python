from etl.db import get_conn

def fetch_all(cur, query: str):
    cur.execute(query)
    return cur.fetchall()

def main():
    conn = get_conn()
    try:
        cur = conn.cursor()

        # Banco atual
        db = fetch_all(cur, "SELECT DATABASE()")[0][0]
        print(f"Banco atual: {db}\n")

        # Contagens
        counts = fetch_all(cur, """
            SELECT 'usuarios' AS tabela, COUNT(*) AS total FROM usuarios
            UNION ALL
            SELECT 'filmes', COUNT(*) FROM filmes
            UNION ALL
            SELECT 'locacoes', COUNT(*) FROM locacoes
        """)
        print("Contagem de registros:")
        for tabela, total in counts:
            print(f"- {tabela}: {total}")
        print()

        # Faturamento mensal (view)
        faturamento = fetch_all(cur, """
            SELECT mes, total_locacoes, faturamento_mes
            FROM vw_faturamento_mensal
            ORDER BY mes
        """)
        print("Faturamento mensal:")
        for mes, total_loc, fat in faturamento:
            print(f"- {mes}: {total_loc} locações | faturamento = {fat}")
        print()

        # Top 5 filmes por faturamento (view)
        top_filmes = fetch_all(cur, """
            SELECT nome_filme, total_locacoes, faturamento_total
            FROM vw_filmes_ranking
            ORDER BY faturamento_total DESC
            LIMIT 5
        """)
        print("Top 5 filmes por faturamento:")
        for nome, total_loc, fat in top_filmes:
            print(f"- {nome}: {total_loc} locações | faturamento = {fat}")
        print()

        # Top 5 usuários por gasto (consulta)
        top_users = fetch_all(cur, """
            SELECT u.nome_usuario,
                   COUNT(*) AS total_locacoes,
                   SUM(l.valor_total) AS gasto_total
            FROM locacoes l
            JOIN usuarios u ON u.id = l.id_usuario
            GROUP BY u.id, u.nome_usuario
            ORDER BY gasto_total DESC
            LIMIT 5
        """)
        print("Top 5 usuários por gasto:")
        for nome, total_loc, gasto in top_users:
            print(f"- {nome}: {total_loc} locações | gasto = {gasto}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
