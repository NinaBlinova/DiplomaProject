# Ф2.а.ii — просмотр данных по месяцам для конкретного налогоплательщика
def test_monthly_data(client):
    response = client.get("/api/dashboard/monthly/100000000041")
    assert response.status_code == 200
    data = response.get_json()
    assert data["success"] is True
    assert "data" in data

# Ф2.а.i — просмотр агрегированных данных по годам
def test_yearly_totals(client):
    response = client.get("/api/dashboard/yearly/totals/100000000041")
    assert response.status_code == 200
    data = response.get_json()
    assert data["success"] is True
    assert "data" in data

# Ф2.б.i — фильтрация по диапазону лет (месячные данные)
def test_monthly_data_with_year_filter(client):
    response = client.get(
        "/api/dashboard/monthly/100000000041",
        query_string={"year": 2022}
    )
    assert response.status_code == 200
    data = response.get_json()
    assert data["success"] is True
    records = data["data"]
    assert all(r["Year"] >= 2022 for r in records)

# Ф2.б.i — фильтрация по диапазону лет (годовые данные)
def test_yearly_totals_with_year_filter(client):
    response = client.get(
        "/api/dashboard/yearly/totals/100000000041",
        query_string={"year": 2022}
    )
    assert response.status_code == 200
    data = response.get_json()
    assert data["success"] is True
    records = data["data"]
    assert all(r["Year"] >= 2022 for r in records)

# Ф2.б.ii — фильтрация по категории налогоплательщика
def test_taxpayers_by_type(client):
    for tax_type in ["SZ", "IP_USN_6", "IP_USN_15", "IP_OSN", "IP_patent"]:
        response = client.get(f"/api/dashboard/taxpayers/{tax_type}")
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "success"
        assert "count" in data

# Ф2.б.i — получение диапазона лет для фильтра
def test_global_year_range(client):
    response = client.get("/api/dashboard/global-year-range")
    assert response.status_code == 200
    data = response.get_json()
    assert data["success"] is True
    assert "min_year" in data["data"]
    assert "max_year" in data["data"]
    assert "years" in data["data"]
    assert data["data"]["min_year"] <= data["data"]["max_year"]

# Ф3.б — расчёт статистических показателей (медиана по месяцам)
def test_monthly_median(client):
    response = client.get(
        "/api/dashboard/monthly/median/SZ",
        query_string={"startYear": 2021, "endYear": 2023}
    )
    assert response.status_code == 200
    data = response.get_json()
    assert data["success"] is True
    assert "data" in data
    record = data["data"][0]
    assert "Income" in record
    assert "Tax" in record
    assert "Transactions" in record

# Ф3.б — расчёт суммарных показателей по месяцам
def test_monthly_general(client):
    response = client.get(
        "/api/dashboard/monthly/general/SZ",
        query_string={"startYear": 2021, "endYear": 2023}
    )
    assert response.status_code == 200
    data = response.get_json()
    assert data["success"] is True
    assert "data" in data

# Ф3.а/б — годовая динамика (суммарная), основа для столбчатых диаграмм
def test_yearly_growth_general(client):
    response = client.get("/api/dashboard/yearly/growth/general/SZ")
    assert response.status_code == 200
    data = response.get_json()
    assert data["success"] is True
    assert "data" in data
    record = data["data"][0]
    assert "Income" in record
    assert "Tax" in record
    assert "Transactions" in record

# Ф3.а/б — годовая динамика (медианная), основа для линейных графиков
def test_yearly_growth_median(client):
    response = client.get("/api/dashboard/yearly/growth/median/SZ")
    assert response.status_code == 200
    data = response.get_json()
    assert data["success"] is True
    assert "data" in data

# Ф3.в — получение результатов прогнозирования
def test_prediction_result(client):
    response = client.get("/api/dashboard/predict_generale/result")
    assert response.status_code == 200
    data = response.get_json()
    assert data["success"] is True
    assert "general" in data
    assert "median" in data
    assert len(data["general"]) > 0
    assert len(data["median"]) > 0


def test_generate_report(client):
    payload = {
        "user": {
            "Id": 14,
            "Username": "admin",
            "FullName": "Администратор Системы"
        },
        "medianData": [
            {
                "Year": 2023,
                "Month": 1,
                "Income": 150000.0,
                "Tax": 9000.0,
                "Transactions": 42,
                "ModelName": "LinearRegression",
                "ModelVersion": "1.0"
            },
            {
                "Year": 2023,
                "Month": 2,
                "Income": 160000.0,
                "Tax": 9600.0,
                "Transactions": 45,
                "ModelName": "LinearRegression",
                "ModelVersion": "1.0"
            }
        ],
        "filters": {
            "taxType": "SZ",
            "inn": "123456789012"
        }
    }
    response = client.post("/api/report", json=payload)
    assert response.status_code == 200
    # Проверяем, что вернулся именно файл Word
    assert "application/vnd.openxmlformats-officedocument" in \
           response.content_type
    # Файл не пустой
    assert len(response.data) > 0

# Ф2.в — экспорт: запрос без данных
def test_generate_report_no_data(client):
    response = client.post("/api/report", json={})
    assert response.status_code == 400
    data = response.get_json()
    assert "error" in data

# Ф2.в — экспорт: пустой medianData
def test_generate_report_empty_median(client):
    payload = {
        "user": {"Id": 14, "Username": "admin", "FullName": "Админ"},
        "medianData": [],
        "filters": {}
    }
    response = client.post("/api/report", json=payload)
    assert response.status_code == 400
    data = response.get_json()
    assert "error" in data