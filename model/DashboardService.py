import pandas as pd


class DashboardService:

    def __init__(self, repository, aggregator, forecaster):
        self.repository = repository
        self.aggregator = aggregator
        self.forecaster = forecaster

    def build_taxpayer_dashboard(self, inn: str, predict_years=1):

        taxpayer = self.repository.get_taxpayer(inn)
        if taxpayer.empty:
            return {"error": "Taxpayer not found"}

        df = self.repository.get_monthly_by_inn(inn)

        last_year = df["Year"].max()

        for i in range(1, predict_years + 1):
            pred = self.forecaster.predict_year(
                taxpayer.iloc[0],
                last_year + i
            )
            df = pd.concat([df, pred])

        yearly_sum = self.aggregator.aggregate_yearly(df, "sum")
        yearly_median = self.aggregator.aggregate_yearly(df, "median")
        growth = self.aggregator.calculate_growth(yearly_sum)

        return {
            "monthly": df,
            "yearly_sum": yearly_sum,
            "yearly_median": yearly_median,
            "growth": growth
        }

    def build_global_dashboard(self, mode="sum"):

        df = self.repository.get_monthly_all()

        monthly = self.aggregator.aggregate_monthly(df, mode)
        yearly = self.aggregator.aggregate_yearly(df, mode)
        growth = self.aggregator.calculate_growth(yearly)

        return {
            "monthly": monthly,
            "yearly": yearly,
            "growth": growth
        }
