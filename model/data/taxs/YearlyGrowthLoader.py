import pandas as pd
from sqlalchemy import text


class YearlyGrowthLoader:

    def __init__(self, db_engine, repository, aggregator, model_name, model_version):
        self.db_engine = db_engine
        self.repository = repository
        self.aggregator = aggregator
        self.model_name = model_name
        self.model_version = model_version

    def _record_exists(self, conn, table_name, year, tax_type):

        if tax_type is None:
            query = text(f"""
                SELECT COUNT(1)
                FROM {table_name}
                WHERE [Year] = ?
                  AND TaxType IS NULL
                  AND ModelName = ?
                  AND ModelVersion = ?
            """)
            result = conn.exec_driver_sql(
                query.text,
                (year, self.model_name, self.model_version)
            ).scalar()
        else:
            query = text(f"""
                SELECT COUNT(1)
                FROM {table_name}
                WHERE [Year] = ?
                  AND TaxType = ?
                  AND ModelName = ?
                  AND ModelVersion = ?
            """)
            result = conn.exec_driver_sql(
                query.text,
                (year, tax_type, self.model_name, self.model_version)
            ).scalar()

        return result > 0

    def _load_growth(self, table_name, aggregation_type, tax_type=None):

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
            print(f"No data for {table_name}")
            return

        yearly_real = self.aggregator.aggregate_yearly(df_real, aggregation_type)
        yearly_pred = self.aggregator.aggregate_yearly(df_pred, aggregation_type)

        combined = pd.concat([yearly_real, yearly_pred], ignore_index=True)
        combined = combined.sort_values("Year")

        growth = self.aggregator.calculate_growth(combined)

        engine = self.db_engine.get_engine()

        with engine.begin() as conn:
            for _, row in growth.iterrows():

                year_value = int(row["Year"])

                if self._record_exists(conn, table_name, year_value, tax_type):
                    print(f"Skip: {year_value} already exists for model {self.model_version}")
                    continue

                insert_sql = f"""
                    INSERT INTO {table_name}
                    ([Year], TaxType,
                     ModelName, ModelVersion,
                     IncomeTotal, TaxTotal, TransactionTotal,
                     IncomeGrowth, TaxGrowth, TransactionsGrowth)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """

                conn.exec_driver_sql(
                    insert_sql,
                    (
                        year_value,
                        tax_type,
                        self.model_name,
                        self.model_version,
                        float(row["Income"]),
                        float(row["Tax"]),
                        float(row["Transactions"]),
                        float(row["IncomeGrowth_%"]) if pd.notna(row["IncomeGrowth_%"]) else 0,
                        float(row["TaxGrowth_%"]) if pd.notna(row["TaxGrowth_%"]) else 0,
                        float(row["TransactionsGrowth_%"]) if pd.notna(row["TransactionsGrowth_%"]) else 0
                    )
                )

        print(f"{table_name} is full for model {self.model_version}")

    def load_general_growth(self, tax_type=None):
        self._load_growth(
            table_name="yearly_growth_general",
            aggregation_type="sum",
            tax_type=tax_type
        )

    def load_median_growth(self, tax_type=None):
        self._load_growth(
            table_name="yearly_growth_median",
            aggregation_type="median",
            tax_type=tax_type
        )
