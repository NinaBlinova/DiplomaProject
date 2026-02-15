from flask import Flask
from flask_cors import CORS

from routes.routes_dashboard import dashboard_bp
from routes.routes_taxpayers import routes_taxpayer


def create_app(test_config=None):
    app = Flask(__name__)

    app.secret_key = 'your-very-secret-key'
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAME SITE'] = 'Lax'
    app.config['SESSION_COOKIE_SECURE'] = False

    if test_config:
        app.config.update(test_config)
    CORS(app)
    app.register_blueprint(routes_taxpayer)
    app.register_blueprint(dashboard_bp)
    return app


if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=5002, debug=True)
