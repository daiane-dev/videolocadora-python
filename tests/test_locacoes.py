def test_listar_locacoes_abertas(client):
    response = client.get("/locacoes/abertas")
    assert response.status_code == 200


def test_buscar_locacao_inexistente(client):
    response = client.get("/locacoes/999999")
    assert response.status_code == 404