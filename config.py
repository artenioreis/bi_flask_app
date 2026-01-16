
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Configuração base da aplicação"""
    SECRET_KEY = os.getenv('SECRET_KEY', 'sua-chave-secreta-aqui')
    DEBUG = False
    TESTING = False

class DevelopmentConfig(Config):
    """Configuração de desenvolvimento"""
    DEBUG = True

class ProductionConfig(Config):
    """Configuração de produção"""
    DEBUG = False

# Configuração padrão
config = DevelopmentConfig()
