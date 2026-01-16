import pyodbc
import json
import os
from pathlib import Path

class DatabaseConnection:
    def __init__(self):
        self.connection = None
        self.config = None
        self.config_file = os.path.join(os.path.dirname(__file__), 'config.json')
        self.load_config()

    def load_config(self):
        """Carrega configuração salva"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    self.config = json.load(f)
                    print(f"✅ Configuração carregada de {self.config_file}")
                    # Tentar conectar automaticamente
                    if self.config:
                        success, msg = self.connect(
                            self.config.get('server'),
                            self.config.get('database'),
                            self.config.get('username'),
                            self.config.get('password')
                        )
                        if success:
                            print(f"✅ Conectado automaticamente ao banco")
        except Exception as e:
            print(f"⚠️  Erro ao carregar configuração: {e}")
            self.config = None

    def save_config(self, server, database, username, password):
        """Salva configuração para uso futuro"""
        try:
            self.config = {
                'server': server,
                'database': database,
                'username': username,
                'password': password
            }
            os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
            print(f"✅ Configuração salva em {self.config_file}")
            return True
        except Exception as e:
            print(f"❌ Erro ao salvar configuração: {e}")
            return False

    def connect(self, server=None, database=None, username=None, password=None):
        """Conecta ao banco de dados"""
        try:
            # Usar parâmetros fornecidos ou carregar da config
            if not server:
                if not self.config:
                    return False, "Nenhuma configuração disponível"
                server = self.config.get('server')
                database = self.config.get('database')
                username = self.config.get('username')
                password = self.config.get('password')

            # Fechar conexão anterior se existir
            if self.connection:
                try:
                    self.connection.close()
                except:
                    pass

            # String de conexão
            connection_string = (
                f'Driver={{ODBC Driver 17 for SQL Server}};'
                f'Server={server};'
                f'Database={database};'
                f'UID={username};'
                f'PWD={password};'
                f'TrustServerCertificate=yes;'
                f'Connection Timeout=10;'
                f'Encrypt=no;'
            )

            print(f"🔄 Conectando a {database}@{server}...")

            # Tentar conectar
            self.connection = pyodbc.connect(connection_string, autocommit=True)

            # Testar a conexão
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()

            print(f"✅ Conectado ao banco: {database}@{server}")
            return True, "Conectado com sucesso"

        except pyodbc.Error as e:
            error_msg = str(e)
            print(f"❌ Erro de conexão ODBC: {error_msg}")
            self.connection = None
            return False, error_msg
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Erro inesperado: {error_msg}")
            self.connection = None
            return False, error_msg

    def is_connected(self):
        """Verifica se está conectado"""
        if not self.connection:
            return False

        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except:
            self.connection = None
            return False

    def reconnect(self):
        """Reconecta ao banco se necessário"""
        if not self.is_connected():
            print("⚠️  Conexão perdida, tentando reconectar...")
            if self.config:
                return self.connect(
                    self.config.get('server'),
                    self.config.get('database'),
                    self.config.get('username'),
                    self.config.get('password')
                )
        return True, "Conectado"

    def execute_query(self, query):
        """Executa uma query e retorna os resultados"""
        try:
            # Verificar e reconectar se necessário
            if not self.is_connected():
                print("⚠️  Reconectando ao banco...")
                success, msg = self.reconnect()
                if not success:
                    print(f"❌ Falha ao reconectar: {msg}")
                    return False, []

            # Criar cursor
            cursor = self.connection.cursor()

            # Executar query
            cursor.execute(query)

            # Buscar resultados
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            results = []

            if columns:
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))

            cursor.close()

            print(f"✅ Query executada - {len(results)} registros")
            return True, results

        except pyodbc.Error as e:
            error_msg = str(e)
            print(f"❌ Erro SQL: {error_msg}")
            print(f"   Query: {query[:100]}...")
            self.connection = None
            return False, []
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Erro ao executar query: {error_msg}")
            return False, []

    def close(self):
        """Fecha a conexão"""
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                print("✅ Conexão fechada")
        except Exception as e:
            print(f"❌ Erro ao fechar conexão: {e}")

# Instância global
db = DatabaseConnection()
