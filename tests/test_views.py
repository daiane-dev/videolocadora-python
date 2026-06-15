def test_views_filmes_ranking(client):
    response = client.get("/views/filmes-ranking")

    assert response.status_code == 200

    data = response.json()

    assert isinstance(data, list)

    if data:
        assert "id_filme" in data[0]
        assert "nome_filme" in data[0]
        assert "total_locacoes" in data[0]
        assert "faturamento_total" in data[0]


def test_views_faturamento_mensal(client):
    response = client.get("/views/faturamento-mensal")

    assert response.status_code == 200

    data = response.json()

    assert isinstance(data, list)

    if data:
        assert "mes" in data[0]
        assert "total_locacoes" in data[0]
        assert "faturamento_mes" in data[0]