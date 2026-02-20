import numpy as np
import pandas as pd
from datetime import datetime
from sqlalchemy import text
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    median_absolute_error,
    max_error
)

from model.database import DatabaseEngine


def calculate_metrics(y_test, y_pred):
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, y_pred)

    # protection against division by 0
    non_zero_mask = y_test != 0
    if np.any(non_zero_mask):
        mape = np.mean(np.abs((y_test[non_zero_mask] - y_pred[non_zero_mask]) /
                              y_test[non_zero_mask])) * 100
    else:
        mape = None

    medae = median_absolute_error(y_test, y_pred)
    maxerr = max_error(y_test, y_pred)

    return {
        "MAE": float(mae),
        "RMSE": float(rmse),
        "MSE": float(mse),
        "R2": float(r2),
        "MAPE": float(mape) if mape is not None else None,
        "MedianAE": float(medae),
        "MaxError": float(maxerr),
        "Observations": int(len(y_test))
    }


def save_metrics_to_db(db_engine, metrics_dict,
                       model_name,
                       target_name,
                       dataset_type="test",
                       model_version="v1.0",
                       tax_type=None):
    engine = db_engine.get_engine()
    if engine is None:
        print("No connection to database")
        return

    insert_query = text("""
        INSERT INTO dbo.model_metrics (
            ModelName,
            TargetName,
            DatasetType,
            MAE,
            RMSE,
            MSE,
            R2,
            MAPE,
            MedianAE,
            MaxError,
            Observations,
            ModelVersion,
            TaxType,
            CreatedAt
        )
        VALUES (
            :ModelName,
            :TargetName,
            :DatasetType,
            :MAE,
            :RMSE,
            :MSE,
            :R2,
            :MAPE,
            :MedianAE,
            :MaxError,
            :Observations,
            :ModelVersion,
            :TaxType,
            :CreatedAt
        )
    """)

    with engine.begin() as conn:
        conn.execute(insert_query, {
            "ModelName": model_name,
            "TargetName": target_name,
            "DatasetType": dataset_type,
            "MAE": metrics_dict["MAE"],
            "RMSE": metrics_dict["RMSE"],
            "MSE": metrics_dict["MSE"],
            "R2": metrics_dict["R2"],
            "MAPE": metrics_dict["MAPE"],
            "MedianAE": metrics_dict["MedianAE"],
            "MaxError": metrics_dict["MaxError"],
            "Observations": metrics_dict["Observations"],
            "ModelVersion": model_version,
            "TaxType": tax_type,
            "CreatedAt": datetime.now()
        })

    print(f"Metrics safe: {model_name} - {target_name}")


db = DatabaseEngine()


def process_model(csv_path, target_name):
    df = pd.read_csv(csv_path)

    y_test = df['y_test'].values
    y_pred = df['y_pred'].values

    metrics = calculate_metrics(y_test, y_pred)

    save_metrics_to_db(
        db_engine=db,
        metrics_dict=metrics,
        model_name="LinearRegression",
        target_name=target_name,
        dataset_type="test",
        model_version="v1.0"
    )


# Income
process_model(
    r"C:\Users\blino\DiplomaProject\model\regression\Linear regression\linear_income_modelpredictions.csv",
    "Income"
)

# Transactions
process_model(
    r"C:\Users\blino\DiplomaProject\model/regression/Linear regression/linear_transactions_modelpredictions.csv",
    "Transactions"
)

# Tax
process_model(
    r"C:\Users\blino\DiplomaProject\model/regression/Linear regression/linear_tax_modelpredictions.csv",
    "Tax"
)

db.dispose_engine()
