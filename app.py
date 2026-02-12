from flask import Flask
from flask_cors import CORS


from routes.routes_taxpayers import routes_taxpayer

app = Flask(__name__)
app.secret_key = 'your-very-secret-key'
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_SECURE'] = False

CORS(app)
app.register_blueprint(routes_taxpayer)

# --- Запуск ---
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, debug=True)