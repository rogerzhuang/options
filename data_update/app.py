from flask import Flask
from flask_socketio import SocketIO
from views import main as main_blueprint
from models import db
from dotenv import load_dotenv
import os

load_dotenv()

db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')
db_server = os.environ.get('DB_SERVER').replace('\\\\', '\\')
db_name = os.environ.get('DB_NAME')


def create_app():
    app = Flask(__name__)
    app.socketio = SocketIO(app)
    driver_name = "ODBC Driver 17 for SQL Server"
    app.config['SQLALCHEMY_DATABASE_URI'] = f'mssql+pyodbc://{db_user}:{db_password}@{db_server}/{db_name}?driver={driver_name}'

    db.init_app(app)
    return app


app = create_app()
app.register_blueprint(main_blueprint)


if __name__ == '__main__':
    app.socketio.run(app, port=5000, debug=True)
