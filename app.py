import os

from flask import Flask
from flask_cors import CORS

from model.AggregationService import AggregationService
from model.ForecastService import ForecastService
from model.TaxDataRepository import TaxDataRepository
from model.YearlyGrowthLoader import YearlyGrowthLoader
from model.YearlyLoader_by_month import YearlyStatsLoader
from model.database import DatabaseEngine
from routes.routes_dashboard import dashboard_bp
from routes.routes_models import models_bp, initialize_predictions
from routes.routes_taxpayers import routes_taxpayer


def create_app(test_config=None):
    app = Flask(__name__)

    app.config["ACTIVE_MODEL_NAME"] = "LightGBM"
    app.config["ACTIVE_MODEL_VERSION"] = "v1.0"

    app.secret_key = 'your-very-secret-key'
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAME SITE'] = 'Lax'
    app.config['SESSION_COOKIE_SECURE'] = False

    if test_config:
        app.config.update(test_config)
    CORS(app)

    app.db_engine = DatabaseEngine()
    app.tax_repository = TaxDataRepository(app.db_engine)
    app.forecaster = ForecastService(
        model_name="LightGBM",
        model_version="v1.0"
    )
    app.aggregator = AggregationService()
    app.yearly_growth_loader = YearlyGrowthLoader(
        app.db_engine,
        app.tax_repository,
        app.aggregator,
        app.forecaster.model_name,
        app.forecaster.model_version
    )
    app.yearly_stats_loader = YearlyStatsLoader(
        app.db_engine,
        app.tax_repository,
        app.aggregator,
        app.forecaster.model_name,
        app.forecaster.model_version
    )
    app.register_blueprint(routes_taxpayer)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(models_bp)

    return app


if __name__ == '__main__':
    app = create_app()
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
        with app.app_context():
            initialize_predictions()
    app.run(host='0.0.0.0', port=5002, debug=True)
