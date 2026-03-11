from etl.db import get_conn


def test_listar_filmes(client):
    response = client.get("/filmes")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_buscar_filme_inexistente(client):
    response = client.get("/filmes/999999")
    assert response.status_code == 404


def test_criar_filme(client):
    payload = {
        "nome_filme": "Filme Teste Pytest",
        "genero_filme": "Drama",
        "ano_filme": 2024
    }

    filme_id = None

    try:
        response = client.post("/filmes", json=payload)

        assert response.status_code == 200

        data = response.json()
        assert data["message"] == "Filme criado com sucesso"
        assert "id" in data
        assert isinstance(data["id"], int)

        filme_id = data["id"]

    finally:
        if filme_id is not None:
            conn = get_conn()
            try:
                cur = conn.cursor()
                cur.execute("DELETE FROM filmes WHERE id = %s", (filme_id,))
                conn.commit()
            finally:
                conn.close()