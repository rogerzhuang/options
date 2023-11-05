from flask import Flask
from flask_socketio import SocketIO
from views import main as main_blueprint
from models import db
import config


def create_app():
    app = Flask(__name__)
    app.socketio = SocketIO(app)
    driver_name = "ODBC Driver 17 for SQL Server"
    app.config['SQLALCHEMY_DATABASE_URI'] = f'mssql+pyodbc://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_SERVER}/{config.DB_NAME}?driver={driver_name}'

    db.init_app(app)
    return app


app = create_app()
app.register_blueprint(main_blueprint)


if __name__ == '__main__':
    app.socketio.run(app, debug=True)
