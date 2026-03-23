import pyodbc
import json
import os

class DatabaseConnection:
    def __init__(self):
        self.connection = None
        self.config_file = os.path.join(os.path.dirname(__file__), 'config.json')
        self.config = self.load_config()

    def load_config(self):
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    return json.load(f)
        except: return None

    def connect(self):
        if not self.config: return False, "Sem config"
        try:
            conn_str = (f"Driver={{ODBC Driver 17 for SQL Server}};Server={self.config['server']};"
                        f"Database={self.config['database']};UID={self.config['username']};PWD={self.config['password']}")
            self.connection = pyodbc.connect(conn_str, autocommit=True)
            return True, "Ok"
        except Exception as e: return False, str(e)

    def execute_query(self, query):
        if not self.connection: self.connect()
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except: return []

db = DatabaseConnection()