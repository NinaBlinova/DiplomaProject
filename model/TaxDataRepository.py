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

    def get_predict_data(self):
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

    def get_monthly_data(
            self,
            source="real",  # "real" | "predict"
            tax_type=None,
            aggregate=False
    ):
        if source == "real":
            table = "MonthlyTaxData"
            income_col = "IncomeAmount"
            trans_col = "transactions_count"
            tax_col = "TaxAmount"
        elif source == "predict":
            table = "Predict"
            income_col = "Income"
            trans_col = "Transactions"
            tax_col = "Tax"
        else:
            raise ValueError("Invalid source type")

        if aggregate:
            select_part = f"""
                SELECT 
                    Year,
                    Month,
                    SUM({income_col}) AS TotalIncome,
                    SUM({trans_col}) AS TotalTransactions,
                    SUM({tax_col}) AS TotalTax
            """
            group_part = "GROUP BY Year, Month"
        else:
            select_part = f"""
                SELECT 
                    Year,
                    Month,
                    {income_col} AS TotalIncome,
                    {trans_col} AS TotalTransactions,
                    {tax_col} AS TotalTax
            """
            group_part = ""

        where_part = ""
        params = []

        if tax_type is not None:
            where_part = "WHERE TaxType = ?"
            params.append(tax_type)

        query = f"""
            {select_part}
            FROM {table}
            {where_part}
            {group_part}
            ORDER BY Year, Month
        """

        return self.db_engine.execute_query(query, params)

    def get_yearly_growth_by_type(self, table_name, tax_type=None, year=2026):
        if tax_type is None:
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE [Year] = ? AND TaxType IS NULL
            """
            params = [year]
        else:
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE [Year] = ? AND TaxType = ?
            """
            params = [year, tax_type]

        df = self.db_engine.execute_query(query, params)

        return None if df.empty else df

