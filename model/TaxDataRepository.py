import pandas as pd


class TaxDataRepository:

    def __init__(self, db_engine):
        self.db_engine = db_engine

    def get_years(self):
        query = "SELECT DISTINCT Year FROM MonthlyTaxData ORDER BY Year"
        return self.db_engine.execute_query(query)

    def get_taxpayers_count(self, tax_type=None):
        if tax_type is None:
            query = "SELECT COUNT(*) FROM Taxpayer"
            result = self.db_engine.execute_query(query)
        else:
            query = "SELECT COUNT(*) FROM Taxpayer WHERE TaxpayerType=?"
            result = self.db_engine.execute_query(query, [tax_type])

        if not result.empty:
            return int(result.iloc[0, 0])
        return 0

    def get_taxpayers(self, tax_type=None):
        if tax_type is None:
            query = """
                    SELECT DISTINCT
                        t.TaxpayerId,
                        t.FullName,
                        t.INN,
                        t.TaxpayerType,
                        t.activity_type,
                        t.registration_district,
                        t.has_employees,
                        t.employees_count,
                        m.TaxType
                    FROM Taxpayer t
                    JOIN MonthlyTaxData m ON t.TaxpayerId = m.TaxpayerId
                    """
            return self.db_engine.execute_query(query)
        else:
            query = """
                    SELECT DISTINCT
                        t.TaxpayerId,
                        t.FullName,
                        t.INN,
                        t.TaxpayerType,
                        t.activity_type,
                        t.registration_district,
                        t.has_employees,
                        t.employees_count,
                        m.TaxType
                    FROM Taxpayer t
                    JOIN MonthlyTaxData m ON t.TaxpayerId = m.TaxpayerId
                    WHERE m.TaxType = ?
                    """
        return self.db_engine.execute_query(query, [tax_type])

    def get_monthly_summary(self, column_name, tax_type=None):
        """
        Universal function for getting monthly taxpayer data
        column_name: 'IncomeAmount', 'transactions_count', 'TaxAmount'
        """
        last_year = self.get_years()  # last year
        if tax_type is None:
            query = f"""
                SELECT SUM([{column_name}]) AS Total
                FROM dbo.MonthlyTaxData
                WHERE [Year] = ?
            """
            params = [last_year]
        else:
            query = f"""
                SELECT SUM([{column_name}]) AS Total
                FROM dbo.MonthlyTaxData
                WHERE [Year] = ? AND TaxType = ?
            """
            params = [last_year, tax_type]
        result = self.db_engine.execute_query(query, params)
        if result.empty or pd.isna(result.iloc[0, 0]):
            total_value = 0.0
        else:
            total_value = float(result.iloc[0, 0])
        return pd.DataFrame([{"Total": total_value}])

    def get_taxpayer(self, inn: str):
        query = "SELECT * FROM Taxpayer WHERE INN = ?"
        return self.db_engine.execute_query(query, [inn])

    def get_monthly_by_inn(self, inn: str):
        query = """
        SELECT m.Year, m.Month, m.IncomeAmount AS TotalIncome,
                m.transactions_count AS TotalTransactions, m.TaxAmount AS TotalTax
        FROM MonthlyTaxData m
        JOIN Taxpayer t ON t.TaxpayerId = m.TaxpayerId
        WHERE t.INN = ?
        ORDER BY m.Year, m.Month
        """
        return self.db_engine.execute_query(query, [inn])

    def get_generale_sum(self, tax_type=None):
        if tax_type is None:
            query = """
            SELECT 
                Year, 
                Month, 
                SUM(IncomeAmount) AS TotalIncome,
                SUM(transactions_count) AS TotalTransactions,
                SUM(TaxAmount) AS TotalTax
            FROM MonthlyTaxData
            GROUP BY Year, Month
            ORDER BY Year, Month
            """
            return self.db_engine.execute_query(query)
        else:
            query = """
            SELECT 
                Year, 
                Month, 
                SUM(IncomeAmount) AS TotalIncome,
                SUM(transactions_count) AS TotalTransactions,
                SUM(TaxAmount) AS TotalTax
            FROM MonthlyTaxData
            WHERE TaxType = ?
            GROUP BY Year, Month
            ORDER BY Year, Month
            """
            params = [tax_type]
            return self.db_engine.execute_query(query, params)

    def get_monthly_tax_income_transaction(self, tax_type=None):
        """
        Get all data from table MonthlyTaxData
        """
        if tax_type is None:
            query = """
                SELECT 
                    Year, 
                    Month, 
                    IncomeAmount AS TotalIncome,
                    transactions_count AS TotalTransactions,
                    TaxAmount AS TotalTax
                FROM MonthlyTaxData
                ORDER BY Year, Month
            """
            return self.db_engine.execute_query(query)
        else:
            query = """
                SELECT 
                    Year, 
                    Month, 
                    IncomeAmount AS TotalIncome,
                    transactions_count AS TotalTransactions,
                    TaxAmount AS TotalTax
                FROM MonthlyTaxData
                WHERE TaxType = ?
                ORDER BY Year, Month
            """
            params = [tax_type]
            return self.db_engine.execute_query(query, params)

    def get_predict_data(self, year=None):
        """
            Returns prediction data from Predict table.
            If year is provided, filters by year.
        """
        query = """
                SELECT 
                    TaxpayerId,
                    FullName,
                    INN,
                    Year,
                    Month,
                    Income,
                    Transactions,
                    Tax,
                    TaxType,
                    TaxpayerType,
                    activity_type,
                    registration_district,
                    has_employees,
                    employees_count
                FROM Predict
            """
        return self.db_engine.execute_query(query)

