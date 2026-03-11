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


def test_register_employee(client):
    response = client.post(
        "/api/admin/register_employee",
        json={
            "admin_id": 14,
            "username": "testemployee",
            "email": "testemployee@mail.com",
            "password": "test123",
            "full_name": "Тестовый Сотрудник",
            "phone": "+79990000000"
        }
    )
    assert response.status_code in [201, 400]
    data = response.get_json()
    assert "success" in data
    assert "message" in data


def test_edit_user(client):
    response = client.put(
        "/api/admin/edit_user",
        json={
            "admin_id": 14,
            "user_id": 16,
            "address_reg": "Москва",
            "gender": "Женский"
        }
    )
    assert response.status_code in [200, 400]
    data = response.get_json()
    assert "success" in data
