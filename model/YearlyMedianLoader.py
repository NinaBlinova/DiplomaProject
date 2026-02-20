import pandas as pd
from datetime import datetime


class YearlyMedianLoader:

    def __init__(self, db_engine, repository, aggregator):
        self.db_engine = db_engine
        self.repository = repository
        self.aggregator = aggregator

    def _median_exists(self, year: int, month: int, tax_type=None):

        if tax_type is None:
            query = """
                SELECT COUNT(*) as Cnt
                FROM dbo.yearly_stats_median
                WHERE [Year] = ?
                  AND [Month] = ?
                  AND TaxType IS NULL
            """
            params = [year, month]
        else:
            query = """
                SELECT COUNT(*) as Cnt
                FROM dbo.yearly_stats_median
                WHERE [Year] = ?
                  AND [Month] = ?
                  AND TaxType = ?
            """
            params = [year, month, tax_type]

        df = self.db_engine.execute_query(query, params)

        return not df.empty and int(df.iloc[0]["Cnt"]) > 0

    def load_monthly_median(self, tax_type=None):

        df_real = self.repository.get_monthly_data(
            source="real",
            tax_type=tax_type,
            aggregate=False
        )

        df_predict = self.repository.get_monthly_data(
            source="predict",
            tax_type=tax_type,
            aggregate=False
        )

        df = pd.concat([df_real, df_predict], ignore_index=True)

        if df.empty:
            print("No data for median (real + predict)")
            return

        median_df = self.aggregator.aggregate_yearly(df, "median")

        if median_df.empty:
            print("No data after aggregation")
            return

        engine = self.db_engine.get_engine()
        rows_to_insert = []

        for _, row in median_df.iterrows():

            year = int(row["Year"])
            month = int(row["Month"])

            if self._median_exists(year, month, tax_type):
                print(f"⚠ Already exists: {year}-{month}, tax_type={tax_type}")
                continue

            rows_to_insert.append({
                "Year": year,
                "Month": month,
                "TaxType": tax_type,
                "IncomeMedian": float(row["Income"]),
                "TaxMedian": float(row["Tax"]),
                "TransactionsMedian": float(row["Transactions"]),
                "CreatedAt": datetime.now()
            })

        if not rows_to_insert:
            print("No new rows to insert")
            return

        insert_df = pd.DataFrame(rows_to_insert)

        insert_df.to_sql(
            "yearly_stats_median",
            engine,
            schema="dbo",
            if_exists="append",
            index=False
        )

        print(f"✅ Inserted rows: {len(insert_df)}")
