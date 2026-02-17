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

