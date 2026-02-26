class ModelsRepository:
    def __init__(self, db_engine):
        self.db_engine = db_engine

    def get_available_models(self):
        query = """
            SELECT DISTINCT
                ModelName,
                ModelVersion,
                TargetName,
                R2,
                MAE,
                RMSE,
                CreatedAt
            FROM model_metrics
            ORDER BY CreatedAt DESC
        """
        return self.db_engine.execute_query(query)
