import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=Taxpayer_Database_DiplomaProject;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()


# Очистка таблицы и сброс IDENTITY
try:
    cursor.execute("DELETE FROM MonthlyTaxData;")
    cursor.execute("DBCC CHECKIDENT ('MonthlyTaxData', RESEED, 0);")
    conn.commit()
    print("Таблица MonthlyTaxData очищена и IDENTITY сброшен.")
except Exception as e:
    print(f"Ошибка при очистке таблицы: {e}")

# try:
#     # Отключаем все внешние ключи, ссылающиеся на Taxpayer
#     cursor.execute("""
#     EXEC sp_msforeachtable "ALTER TABLE ? NOCHECK CONSTRAINT ALL"
#     """)
#     conn.commit()
#
#     # Удаляем все записи
#     cursor.execute("DELETE FROM Taxpayer;")
#     conn.commit()
#     print("Таблица Taxpayer очищена через DELETE.")
#
#     # Сбрасываем IDENTITY
#     cursor.execute("DBCC CHECKIDENT ('Taxpayer', RESEED, 0);")
#     conn.commit()
#     print("Счетчик IDENTITY сброшен.")
#
#     # Включаем все внешние ключи обратно
#     cursor.execute("""
#     EXEC sp_msforeachtable "ALTER TABLE ? WITH CHECK CHECK CONSTRAINT ALL"
#     """)
#     conn.commit()
#     print("Внешние ключи восстановлены.")
#
# except Exception as e:
#     print(f"Ошибка при очистке таблицы: {e}")

cursor.close()
conn.close()
