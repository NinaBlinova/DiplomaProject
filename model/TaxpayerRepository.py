import pandas as pd
from typing import Optional

class TaxpayerRepository():
    def __init__(self, db_engine):
        self.db_engine = db_engine

    def get_taxpayers_paginated_raw(
            self,
            page: int,
            page_size: int,
            inn_filter: Optional[str],
            district_filter: Optional[str],
            sort_by: str,
            sort_order: str
    ) -> pd.DataFrame:

        offset = (page - 1) * page_size

        query = """
            SELECT 
                TaxpayerId,
                FullName,
                INN,
                registration_district,
                has_employees,
                employees_count,
                COUNT(*) OVER() as total_count
            FROM Taxpayer
            WHERE 1=1
        """

        params = []

        if inn_filter:
            query += " AND INN LIKE ?"
            params.append(f"%{inn_filter}%")

        if district_filter:
            query += " AND registration_district = ?"
            params.append(district_filter)

        query += f" ORDER BY {sort_by} {sort_order} OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
        params.extend([offset, page_size])

        return self.db_engine.execute_query(query, params=params)

    def get_taxpayer_by_inn_raw(self, inn: str) -> pd.DataFrame:

        query = """
            SELECT 
                TaxpayerId,
                FullName,
                PassportNumber,
                INN,
                TaxpayerType,
                activity_type,
                registration_district,
                has_employees,
                employees_count
            FROM Taxpayer
            WHERE INN = ?
        """

        return self.db_engine.execute_query(query, params=[inn])