import pandas as pd
from sqlalchemy import text

class YearlyGrowthLoader:

    def __init__(self, db_engine, repository, aggregator):
        self.db_engine = db_engine
        self.repository = repository
        self.aggregator = aggregator

    def _record_exists(self, conn, table_name, year, tax_type):
        if tax_type is None:
            query = text(f"""
                SELECT COUNT(1)
                FROM {table_name}
                WHERE [Year] = :year
                AND TaxType IS NULL
            """)
            result = conn.execute(query, {"year": year}).scalar()
        else:
            query = text(f"""
                SELECT COUNT(1)
                FROM {table_name}
                WHERE [Year] = :year
                AND TaxType = :taxtype
            """)
            result = conn.execute(query, {
                "year": year,
                "taxtype": tax_type
            }).scalar()

        return result > 0

    def load_general_growth(self, tax_type=None):
        df_real = self.repository.get_monthly_data(
            source="real",
            tax_type=tax_type,
            aggregate=False
        )

        df_pred = self.repository.get_monthly_data(
            source="predict",
            tax_type=tax_type,
            aggregate=False
        )

        if df_real.empty and df_pred.empty:
            print("No data for general")
            return
        yearly_real = self.aggregator.aggregate_yearly(df_real, "sum")
        yearly_pred = self.aggregator.aggregate_yearly(df_pred, "sum")
        combined = pd.concat([yearly_real, yearly_pred], ignore_index=True)
        combined = combined.sort_values("Year")
        growth = self.aggregator.calculate_growth(combined)
        engine = self.db_engine.get_engine()
        with engine.begin() as conn:
            for _, row in growth.iterrows():

                year_value = int(row["Year"])

                if self._record_exists(conn, "yearly_growth_general", year_value, tax_type):
                    print(f"Skip: {year_value} already exists")
                    continue

                conn.execute(text("""
                    INSERT INTO yearly_growth_general
                    ([Year], TaxType,
                     IncomeTotal, TaxTotal, TransactionTotal,
                     IncomeGrowth, TaxGrowth, TransactionsGrowth)
                    VALUES
                    (:year, :taxtype,
                     :income_total, :tax_total, :trans_total,
                     :income_growth, :tax_growth, :trans_growth)
                """), {
                    "year": int(row["Year"]),
                    "taxtype": tax_type,
                    "income_total": float(row["Income"]),
                    "tax_total": float(row["Tax"]),
                    "trans_total": float(row["Transactions"]),
                    "income_growth": float(row["IncomeGrowth_%"]) if pd.notna(row["IncomeGrowth_%"]) else 0,
                    "tax_growth": float(row["TaxGrowth_%"]) if pd.notna(row["TaxGrowth_%"]) else 0,
                    "trans_growth": float(row["TransactionsGrowth_%"]) if pd.notna(row["TransactionsGrowth_%"]) else 0
                })

        print("yearly_growth_general is full")

    def load_median_growth(self, tax_type=None):
        df_real = self.repository.get_monthly_data(
            source="real",
            tax_type=tax_type,
            aggregate=False
        )

        df_pred = self.repository.get_monthly_data(
            source="predict",
            tax_type=tax_type,
            aggregate=False
        )

        if df_real.empty and df_pred.empty:
            print("No data for median")
            return

        yearly_real = self.aggregator.aggregate_yearly(df_real, "median")
        yearly_pred = self.aggregator.aggregate_yearly(df_pred, "median")

        combined = pd.concat([yearly_real, yearly_pred], ignore_index=True)
        combined = combined.sort_values("Year")

        growth = self.aggregator.calculate_growth(combined)

        engine = self.db_engine.get_engine()

        with engine.begin() as conn:
            for _, row in growth.iterrows():
                year_value = int(row["Year"])

                if self._record_exists(conn, "yearly_growth_median", year_value, tax_type):
                    print(f"Skip: {year_value} already exists")
                    continue
                conn.execute(text("""
                    INSERT INTO yearly_growth_median
                    ([Year], TaxType,
                     IncomeTotal, TaxTotal, TransactionTotal,
                     IncomeGrowth, TaxGrowth, TransactionsGrowth)
                    VALUES
                    (:year, :taxtype,
                     :income_total, :tax_total, :trans_total,
                     :income_growth, :tax_growth, :trans_growth)
                """), {
                    "year": int(row["Year"]),
                    "taxtype": tax_type,
                    "income_total": float(row["Income"]),
                    "tax_total": float(row["Tax"]),
                    "trans_total": float(row["Transactions"]),
                    "income_growth": float(row["IncomeGrowth_%"]) if pd.notna(row["IncomeGrowth_%"]) else 0,
                    "tax_growth": float(row["TaxGrowth_%"]) if pd.notna(row["TaxGrowth_%"]) else 0,
                    "trans_growth": float(row["TransactionsGrowth_%"]) if pd.notna(row["TransactionsGrowth_%"]) else 0
                })

        print("yearly_growth_median full")
