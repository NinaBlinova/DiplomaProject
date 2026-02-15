import pandas as pd


class TaxpayerService:

    def __init__(self, repository):
        self.repository = repository

    # pagination
    def get_taxpayers_paginated(
            self,
            page: int,
            page_size: int,
            inn_filter=None,
            district_filter=None,
            sort_by='TaxpayerId',
            sort_order='ASC'
    ):

        df = self.repository.get_taxpayers_paginated_raw(
            page,
            page_size,
            inn_filter,
            district_filter,
            sort_by,
            sort_order
        )

        if not isinstance(df, pd.DataFrame) or df.empty:
            return {
                'data': pd.DataFrame(),
                'total': 0,
                'page': page,
                'page_size': page_size,
                'total_pages': 0
            }

        total_count = int(df['total_count'].iloc[0])
        data_df = df.drop('total_count', axis=1).copy()

        # normalization of types
        data_df['TaxpayerId'] = data_df['TaxpayerId'].astype(int)
        data_df['has_employees'] = data_df['has_employees'].astype(bool)
        data_df['employees_count'] = data_df['employees_count'].apply(
            lambda x: int(x) if pd.notna(x) else None
        )

        return {
            'data': data_df,
            'total': total_count,
            'page': page,
            'page_size': page_size,
            'total_pages': (total_count + page_size - 1) // page_size
        }

    # find by inn
    def get_taxpayer_by_inn(self, inn: str):

        df = self.repository.get_taxpayer_by_inn_raw(inn)

        if df.empty:
            return pd.DataFrame()

        df['TaxpayerId'] = df['TaxpayerId'].astype(int)
        df['has_employees'] = df['has_employees'].astype(bool)
        df['employees_count'] = df['employees_count'].apply(
            lambda x: int(x) if pd.notna(x) else None
        )

        return df
