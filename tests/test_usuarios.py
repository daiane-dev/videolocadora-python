from etl.db import get_conn


def test_listar_usuarios(client):
    response = client.get("/usuarios")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_criar_usuario(client):
    payload = {
        "nome_usuario": "Usuario Teste Pytest",
        "idade": 30,
        "cidade": "Limeira",
        "estado": "SP"
    }

    usuario_id = None

    try:
        response = client.post("/usuarios", json=payload)

        assert response.status_code == 200

        data = response.json()

        assert data["message"] == "Usuário criado com sucesso"

        assert "id_usuario" in data
        assert isinstance(data["id_usuario"], int)

        usuario_id = data["id_usuario"]

    finally:
        if usuario_id is not None:
            conn = get_conn()

            try:
                cur = conn.cursor()

                cur.execute(
                    "DELETE FROM usuarios WHERE id = %s",
                    (usuario_id,)
                )

                conn.commit()

            finally:
                conn.close()