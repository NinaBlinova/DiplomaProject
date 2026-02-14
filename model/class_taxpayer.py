from sqlalchemy import text
import pandas as pd
from typing import Optional, Dict, Any


class TaxpayerRepository:
    def __init__(self, db_engine):
        """
        :param db_engine: экземпляр DatabaseEngine
        """
        self.db_engine = db_engine

    def get_taxpayers_paginated(self,
                                page: int = 1,
                                page_size: int = 50,
                                inn_filter: Optional[str] = None,
                                district_filter: Optional[str] = None,
                                sort_by: str = 'TaxpayerId',
                                sort_order: str = 'ASC'):

        offset = (page - 1) * page_size

        # Используем позиционные параметры (?) для совместимости с pyodbc
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

        # SQL Server пагинация
        query += f" ORDER BY {sort_by} {sort_order} OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
        params.extend([offset, page_size])

        df = self.db_engine.execute_query(query, params=params)

        # Преобразуем numpy типы в стандартные Python типы
        if isinstance(df, pd.DataFrame) and not df.empty:
            total_count = int(df['total_count'].iloc[0])  # Конвертируем в int

            # Конвертируем все колонки в стандартные типы Python
            data_df = df.drop('total_count', axis=1).copy()

            # Конвертируем has_employees в bool
            if 'has_employees' in data_df.columns:
                data_df['has_employees'] = data_df['has_employees'].astype(bool)

            # Конвертируем employees_count в int или None
            if 'employees_count' in data_df.columns:
                data_df['employees_count'] = data_df['employees_count'].apply(
                    lambda x: int(x) if pd.notna(x) else None
                )
        else:
            total_count = 0
            data_df = pd.DataFrame()

        return {
            'data': data_df,
            'total': total_count,
            'page': page,
            'page_size': page_size,
            'total_pages': (total_count + page_size - 1) // page_size if total_count > 0 else 0
        }

    def get_taxpayer_by_inn(self, inn: str):
        """
        Получить налогоплательщика по ИНН
        :param inn: ИНН (12 символов)
        :return: DataFrame
        """
        # Используем execute_query для единообразия
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

        try:
            df = self.db_engine.execute_query(query, params=[inn])

            if not df.empty:
                # Конвертируем типы данных
                df['has_employees'] = df['has_employees'].astype(bool)
                df['employees_count'] = df['employees_count'].apply(
                    lambda x: int(x) if pd.notna(x) else None
                )
                df['TaxpayerId'] = df['TaxpayerId'].astype(int)
                # print(df)

            return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

        except Exception as e:
            print(f"Ошибка при получении налогоплательщика по ИНН: {e}")
            return pd.DataFrame()