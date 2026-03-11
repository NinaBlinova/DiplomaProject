import pandas as pd
from typing import Optional


class UserRepository:
    def __init__(self, db_engine):
        self.db_engine = db_engine

    def get_users(
            self,
            username_filter: Optional[str] = None,
            role_filter: Optional[str] = None,
            is_active_filter: Optional[int] = None,
            sort_by: str = "Id",
            sort_order: str = "ASC"
    ) -> pd.DataFrame:

        query = """
            SELECT
                Id,
                Username,
                Email,
                FullName,
                Bio,
                CreatedAt,
                IsActive,
                user_role,
                PassportSeries,
                PassportNumber,
                PassportIssuedBy,
                PassportIssueDate,
                SNILS,
                INN,
                OMSPolicyNumber,
                BirthDate,
                Gender,
                Address_Reg,
                Phone
            FROM Users u
            WHERE 1=1
        """
        params = []
        if username_filter:
            query += " AND u.Username LIKE ?"
            params.append(f"%{username_filter}%")
        if role_filter:
            query += " AND u.user_role = ?"
            params.append(role_filter)
        if is_active_filter is not None:
            query += " AND u.IsActive = ?"
            params.append(is_active_filter)
        allowed_sort_columns = [
            "Id", "Username", "Email", "FullName",
            "user_role", "IsActive", "CreatedAt"
        ]
        if sort_by not in allowed_sort_columns:
            sort_by = "Id"
        if sort_order.upper() not in ["ASC", "DESC"]:
            sort_order = "ASC"
        query += f" ORDER BY {sort_by} {sort_order}"
        return self.db_engine.execute_query(query, params=params)

    def get_user_log(self, user_id: int, limit: Optional[int] = 100) -> pd.DataFrame:
        query = f"""
                    SELECT TOP {limit}
                        Id AS LogId,
                        UserId,
                        Username,
                        Action,
                        AdditionalInfo,
                        ActionDate
                    FROM Logs
                    WHERE UserId = ?
                    ORDER BY ActionDate DESC
                """
        params = [user_id]
        return self.db_engine.execute_query(query, params=params)
