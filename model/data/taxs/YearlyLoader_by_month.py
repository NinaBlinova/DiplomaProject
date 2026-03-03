import pandas as pd
from datetime import datetime


class YearlyStatsLoader:

    def __init__(self, db_engine, repository, aggregator, model_name, model_version):
        self.db_engine = db_engine
        self.repository = repository
        self.aggregator = aggregator
        self.model_name = model_name
        self.model_version = model_version

    def _record_exists(self, table_name: str, year: int, month: int, tax_type=None):

        if tax_type is None:
            query = f"""
                SELECT COUNT(*) as Cnt
                FROM dbo.{table_name}
                WHERE [Year] = ?
                  AND [Month] = ?
                  AND ModelName = ?
                  AND ModelVersion = ?
                  AND TaxType IS NULL
            """
            params = [year, month, self.model_name, self.model_version]

        else:
            query = f"""
                SELECT COUNT(*) as Cnt
                FROM dbo.{table_name}
                WHERE [Year] = ?
                  AND [Month] = ?
                  AND TaxType = ?
                  AND ModelName = ?
                  AND ModelVersion = ?
            """
            params = [year, month, tax_type,
                      self.model_name, self.model_version]

        df = self.db_engine.execute_query(query, params)
        return not df.empty and int(df.iloc[0]["Cnt"]) > 0

    def load_monthly(self, mode: str, tax_type=None):
        """
        mode: 'median' или 'sum'
        """

        if mode not in ["median", "sum"]:
            raise ValueError("Mode must be 'median' or 'sum'")

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
            print(f"No data for {mode} (real + predict)")
            return

        result_df = self.aggregator.aggregate_monthly(df, mode)

        if result_df.empty:
            print("No data after aggregation")
            return

        if mode == "median":
            table_name = "yearly_stats_median"
            income_col = "IncomeMedian"
            tax_col = "TaxMedian"
            trans_col = "TransactionsMedian"
        else:
            table_name = "yearly_stats_general"
            income_col = "IncomeGeneral"
            tax_col = "TaxGeneral"
            trans_col = "TransactionsGeneral"

        engine = self.db_engine.get_engine()
        rows_to_insert = []

        for _, row in result_df.iterrows():
            year = int(row["Year"])
            month = int(row["Month"])

            if self._record_exists(table_name, year, month, tax_type):
                print(f"⚠ Already exists for model {self.model_name} {self.model_version}: {year}-{month}")
                continue

            rows_to_insert.append({
                "Year": year,
                "Month": month,
                "TaxType": tax_type,
                income_col: float(row["Income"]),
                tax_col: float(row["Tax"]),
                trans_col: float(row["Transactions"]),
                "ModelName": self.model_name,
                "ModelVersion": self.model_version,
                "CreatedAt": datetime.now()
            })

        if not rows_to_insert:
            print("No new rows to insert")
            return

        insert_df = pd.DataFrame(rows_to_insert)

        insert_df.to_sql(
            table_name,
            engine,
            schema="dbo",
            if_exists="append",
            index=False
        )

        print(f"Inserted rows into {table_name}: {len(insert_df)}")
