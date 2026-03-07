import datetime

class LoggerService:
    def __init__(self, db_engine):
        self.db = db_engine

    def log_action(self, user_id, username, action, additional_info=None):
        query = """
        INSERT INTO [dbo].[Logs] (UserId, Username, Action, ActionDate, AdditionalInfo)
        VALUES (:user_id, :username, :action, :action_date, :additional_info)
        """
        params = {
            "user_id": user_id,
            "username": username,
            "action": action,
            "action_date": datetime.datetime.now(),
            "additional_info": additional_info
        }
        try:
            self.db.execute_non_query(query, params)
            return True
        except Exception as e:
            print(f"Logging error: {e}")
            return False