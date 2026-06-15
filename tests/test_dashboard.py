def test_dashboard_resumo(client):
    response = client.get("/dashboard/resumo")

    assert response.status_code == 200

    data = response.json()

    assert "total_usuarios" in data
    assert "total_filmes_ativos" in data
    assert "total_locacoes" in data
    assert "faturamento_total" in data


def test_dashboard_faturamento(client):
    response = client.get("/dashboard/faturamento")

    assert response.status_code == 200

    data = response.json()

    assert "periodo" in data
    assert "total_locacoes" in data
    assert "faturamento_total" in data
    assert "locacoes_abertas" in data
    assert "locacoes_devolvidas" in data
    assert "locacoes_atrasadas" in data


def test_dashboard_faturamento_serie(client):
    response = client.get("/dashboard/faturamento-serie?granularidade=dia")

    assert response.status_code == 200

    data = response.json()

    assert isinstance(data, list)

    if data:
        assert "periodo" in data[0]
        assert "total_locacoes" in data[0]
        assert "faturamento_total" in data[0]


def test_dashboard_faturamento_por_genero(client):
    response = client.get("/dashboard/faturamento-por-genero")

    assert response.status_code == 200

    data = response.json()

    assert isinstance(data, list)

    if data:
        assert "genero" in data[0]
        assert "total_locacoes" in data[0]
        assert "faturamento_total" in data[0]