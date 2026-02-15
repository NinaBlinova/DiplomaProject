class AggregationService:

    @staticmethod
    def aggregate_yearly(df, mode="sum"):
        agg = "sum" if mode == "sum" else "median"

        return df.groupby("Year").agg(
            Income=("Income", agg),
            Transactions=("Transactions", agg),
            Tax=("Tax", agg)
        ).reset_index()

    @staticmethod
    def aggregate_monthly(df, mode="sum"):
        agg = "sum" if mode == "sum" else "median"

        return df.groupby(["Year", "Month"]).agg(
            Income=("Income", agg),
            Transactions=("Transactions", agg),
            Tax=("Tax", agg)
        ).reset_index()

    @staticmethod
    def calculate_growth(yearly_df):
        yearly_df = yearly_df.sort_values("Year").copy()

        yearly_df["IncomeGrowth_%"] = yearly_df["Income"].pct_change() * 100
        yearly_df["TransactionsGrowth_%"] = yearly_df["Transactions"].pct_change() * 100
        yearly_df["TaxGrowth_%"] = yearly_df["Tax"].pct_change() * 100

        return yearly_df
