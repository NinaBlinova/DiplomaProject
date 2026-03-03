def test_global_year_range(client):
    response = client.get("/api/dashboard/global-year-range")
    assert response.status_code == 200
    data = response.get_json()
    assert data["success"] is True
    assert "data" in data


def test_taxpayers(client):
    response = client.get("/api/dashboard/taxpayers/SZ")
    assert response.status_code == 200
    data = response.get_json()
    assert data["status"] == 'success'
    assert "data" == 'data'


def test_login_success(client):
    response = client.post(
        "/api/auth/login",
        json={
            "username": "ninablinova",
            "password": "nina123"
        }
    )

    assert response.status_code == 200
    data = response.get_json()
    assert data["success"] is True
    assert "user" in data
    assert data["user"]["Username"] == "ninablinova"


def test_login_wrong_password(client):
    response = client.post(
        "/api/auth/login",
        json={
            "username": "ninablinova",
            "password": "wrongpassword"
        }
    )
    assert response.status_code == 401
    data = response.get_json()
    assert data["success"] is False


def test_login_user_not_found(client):
    response = client.post(
        "/api/auth/login",
        json={
            "username": "notexists",
            "password": "123"
        }
    )
    assert response.status_code == 401
    data = response.get_json()
    assert data["success"] is False
