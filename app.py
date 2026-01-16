from flask import Flask, render_template, request, jsonify, session, redirect, url_for, send_from_directory
from flask_session import Session
import pyodbc
import json
import os
import logging
from datetime import datetime, timedelta
from functools import wraps
import traceback

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'sua_chave_secreta_aqui_2026'
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)

# Variáveis globais
db_connection = None
config_data = {}

# ============================================
# FUNÇÕES DE BANCO DE DADOS
# ============================================

def load_config():
    """Carrega configuração do banco de dados"""
    global config_data
    config_path = os.path.join(app.root_path, 'database', 'config.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
            logger.info(f"✅ Configuração carregada de {config_path}")
            return config_data
    except Exception as e:
        logger.error(f"❌ Erro ao carregar configuração: {e}")
        return None

def connect_to_database():
    """Conecta ao banco de dados SQL Server"""
    global db_connection
    try:
        if config_data:
            connection_string = (
                f"Driver={{ODBC Driver 17 for SQL Server}};"
                f"Server={config_data.get('server')};"
                f"Database={config_data.get('database')};"
                f"UID={config_data.get('username')};"
                f"PWD={config_data.get('password')};"
            )
            logger.info(f"🔄 Conectando a {config_data.get('database')}...")
            db_connection = pyodbc.connect(connection_string)
            logger.info(f"✅ Conectado ao banco: {config_data.get('database')}")
            return db_connection
    except Exception as e:
        logger.error(f"❌ Erro ao conectar: {e}")
        return None

def execute_query(query, params=None):
    """Executa query com reconexão automática"""
    global db_connection
    try:
        if db_connection is None:
            logger.warning("⚠️  Reconectando ao banco...")
            connect_to_database()

        cursor = db_connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        logger.debug(f"Query executada: {query[:100]}...")
        logger.info(f"✅ Query executada - {cursor.rowcount} registros")
        return cursor.fetchall()
    except pyodbc.Error as e:
        logger.error(f"❌ Erro SQL: {e}")
        try:
            db_connection = connect_to_database()
        except:
            pass
        return []
    except Exception as e:
        logger.error(f"❌ Erro inesperado: {e}")
        return []

def format_currency(value):
    """Formata valor para moeda, tratando NULL"""
    if value is None:
        return "R$ 0,00"
    try:
        return f"R$ {float(value):,.2f}".replace(',', '_').replace('.', ',').replace('_', '.')
    except:
        return "R$ 0,00"

# ============================================
# DECORADORES
# ============================================

def login_required(f):
    """Verifica se usuário está autenticado"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# ============================================
# FILTROS JINJA2
# ============================================

@app.template_filter('currency')
def currency_filter(value):
    """Filtro para formatar moeda no template"""
    return format_currency(value)

@app.template_filter('safe_number')
def safe_number_filter(value):
    """Filtro para números seguros"""
    if value is None:
        return 0
    try:
        return float(value)
    except:
        return 0

# ============================================
# ROTAS - AUTENTICAÇÃO
# ============================================

@app.route('/')
def index():
    """Redireciona para dashboard ou login"""
    if 'user' in session:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Página de login"""
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()

        if username and password:
            session['user'] = username
            logger.info(f"✅ Usuário {username} autenticado")
            return redirect(url_for('dashboard'))
        else:
            logger.warning("⚠️  Tentativa de login falhou - credenciais vazias")

    return render_template('login.html')

@app.route('/logout')
def logout():
    """Faz logout do usuário"""
    session.pop('user', None)
    logger.info("✅ Usuário desconectado")
    return redirect(url_for('login'))

# ============================================
# ROTAS - DASHBOARD
# ============================================

@app.route('/dashboard')
@login_required
def dashboard():
    """Dashboard principal"""
    try:
        filtro = request.args.get('tipo', 'todos')
        valor = request.args.get('valor', '').strip()

        logger.info(f"Dashboard - Filtro: {filtro}, Valor: {valor}")

        # Query de clientes
        query_clientes = """
        DECLARE @DatIni DATETIME = '01-01-2026 00:00:00';
        DECLARE @DatFim DATETIME = '31-12-2026 23:59:59';
        SELECT
            cl.Codigo,
            cl.Razao_Social,
            CASE WHEN cl.Bloqueado = '0' THEN 'Não' ELSE 'Sim' END AS Bloqueado,
            ISNULL(cl.Limite_Credito, 0) AS Limite_Credito,
            ISNULL(cl.Total_Debito, 0) AS Total_Debito,
            ISNULL(cl.Valor_UltimaFatura, 0) AS Valor_UltimaFatura,
            ISNULL(cl.Maior_Atraso, 0) AS Maior_Atraso,
            cl.Data_UltimaFatura,
            ISNULL(ve.Nome_guerra, 'N/A') AS Vendedor,
            (SELECT COUNT(DISTINCT Num_Nota) FROM NFSCB
             WHERE Cod_Cliente = cl.Codigo
             AND Status = 'F'
             AND Dat_Emissao BETWEEN @DatIni AND @DatFim
             AND Cod_Estabe = 0) AS Total_NF,
            (SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB
             WHERE Cod_Cliente = cl.Codigo
             AND Status = 'F'
             AND Dat_Emissao BETWEEN @DatIni AND @DatFim
             AND Cod_Estabe = 0) AS Total_Compras
        FROM clien cl
        LEFT JOIN enxes en ON cl.Cgc_Cpf = en.Num_CgcCpf AND en.Cod_Estabe = 0
        LEFT JOIN vende ve ON en.Cod_Vendedor = ve.Codigo AND ve.Bloqueado = 0
        WHERE 1=1
        """

        if filtro == 'vendedor' and valor:
            try:
                cod_vendedor = int(valor)
                query_clientes += f" AND en.Cod_Vendedor = {cod_vendedor}"
                logger.info(f"Filtro vendedor aplicado: {cod_vendedor}")
            except:
                logger.warning(f"⚠️  Valor de vendedor inválido: {valor}")

        if filtro == 'grupo' and valor:
            try:
                cod_grupo = int(valor)
                query_clientes += f" AND cl.Cod_GrpCli = {cod_grupo}"
                logger.info(f"Filtro grupo aplicado: {cod_grupo}")
            except:
                logger.warning(f"⚠️  Valor de grupo inválido: {valor}")

        query_clientes += " ORDER BY cl.Razao_Social"

        clientes = execute_query(query_clientes)
        logger.info(f"Clientes encontrados: {len(clientes)}")

        # Query de vendedores
        query_vendedores = """
        SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado = 0 ORDER BY Nome_guerra
        """
        vendedores = execute_query(query_vendedores)
        logger.info(f"Vendedores encontrados: {len(vendedores)}")

        # Query de grupos
        query_grupos = """
        SELECT DISTINCT cl.Cod_GrpCli
        FROM clien cl
        WHERE cl.Cod_GrpCli IS NOT NULL
        ORDER BY cl.Cod_GrpCli
        """
        grupos = execute_query(query_grupos)
        logger.info(f"Grupos encontrados: {len(grupos)}")

        return render_template('dashboard.html',
                             clientes=clientes,
                             vendedores=vendedores,
                             grupos=grupos,
                             filtro_ativo=filtro,
                             valor_filtro=valor)

    except Exception as e:
        logger.error(f"❌ Erro no dashboard: {e}\n{traceback.format_exc()}")
        return render_template('dashboard.html', 
                             clientes=[], 
                             vendedores=[], 
                             grupos=[],
                             filtro_ativo='todos',
                             valor_filtro='')


# ============================================
# ROTAS - ANÁLISE DE CLIENTE
# ============================================

@app.route('/analise/<int:cliente_id>')
@login_required
def analise_cliente(cliente_id):
    """Análise detalhada do cliente"""
    try:
        # Query do cliente
        query_cliente = f"""
        SELECT TOP 1
            cl.Codigo,
            cl.Razao_Social,
            ISNULL(cl.Limite_Credito, 0) AS Limite_Credito,
            ISNULL(cl.Total_Debito, 0) AS Total_Debito,
            ISNULL(cl.Valor_UltimaFatura, 0) AS Valor_UltimaFatura,
            ISNULL(cl.Maior_Atraso, 0) AS Maior_Atraso,
            cl.Data_UltimaFatura,
            ISNULL(ve.Nome_guerra, 'N/A') AS Vendedor
        FROM clien cl
        LEFT JOIN enxes en ON cl.Cgc_Cpf = en.Num_CgcCpf AND en.Cod_Estabe = 0
        LEFT JOIN vende ve ON en.Cod_Vendedor = ve.Codigo
        WHERE cl.Codigo = {cliente_id}
        """

        cliente = execute_query(query_cliente)

        if not cliente:
            return render_template('404.html'), 404

        cliente = cliente[0]

        # Query de produtos - CORRIGIDA COM COLUNAS CORRETAS
        query_produtos = f"""
        SELECT TOP 20
            it.Cod_Produto,
            ISNULL(pr.Descricao, 'Sem descrição') AS Descricao,
            SUM(ISNULL(it.Qtd_Produto, 0)) AS Quantidade,
            SUM(ISNULL(it.Vlr_TotItem, 0)) AS Valor_Total,
            COUNT(DISTINCT cb.Num_Nota) AS Num_Compras
        FROM NFSCB cb
        INNER JOIN NFSIT it ON cb.Num_Nota = it.Num_Nota AND cb.Ser_Nota = it.Ser_Nota
        LEFT JOIN produ pr ON it.Cod_Produto = pr.Codigo
        WHERE cb.Cod_Cliente = {cliente_id}
        AND cb.Status = 'F'
        GROUP BY it.Cod_Produto, pr.Descricao
        ORDER BY SUM(ISNULL(it.Vlr_TotItem, 0)) DESC
        """

        produtos = execute_query(query_produtos)

        # Calcular análise ABC
        total_vendas = sum([p[3] if p[3] else 0 for p in produtos]) if produtos else 0
        produtos_abc = []
        acumulado = 0

        for produto in produtos:
            acumulado += produto[3] if produto[3] else 0
            percentual = (acumulado / total_vendas * 100) if total_vendas > 0 else 0

            if percentual <= 80:
                classe = 'A'
            elif percentual <= 95:
                classe = 'B'
            else:
                classe = 'C'

            produtos_abc.append({
                'codigo': produto[0],
                'descricao': produto[1],
                'quantidade': produto[2],
                'valor': produto[3],
                'compras': produto[4],
                'classe': classe,
                'percentual': percentual
            })

        # Query de comparativo mensal (2025 vs 2026)
        query_comparativo = f"""
        SELECT 
            DATENAME(MONTH, cb.Dat_Emissao) AS Mes,
            MONTH(cb.Dat_Emissao) AS NumMes,
            YEAR(cb.Dat_Emissao) AS Ano,
            ISNULL(SUM(cb.Vlr_TotalNota), 0) AS Total_Vendas
        FROM NFSCB cb
        WHERE cb.Cod_Cliente = {cliente_id}
        AND cb.Status = 'F'
        AND YEAR(cb.Dat_Emissao) IN (2025, 2026)
        GROUP BY DATENAME(MONTH, cb.Dat_Emissao), MONTH(cb.Dat_Emissao), YEAR(cb.Dat_Emissao)
        ORDER BY NumMes, Ano
        """

        comparativo_raw = execute_query(query_comparativo)

        # Processar dados do comparativo
        comparativo = {}
        meses_nomes = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ']

        for row in comparativo_raw:
            mes_num = row[1]
            ano = row[2]
            valor = row[3]

            if mes_num not in comparativo:
                comparativo[mes_num] = {'mes': meses_nomes[mes_num - 1], '2025': 0, '2026': 0}

            comparativo[mes_num][str(ano)] = valor

        # Converter para lista ordenada
        comparativo_lista = [comparativo[i] for i in sorted(comparativo.keys())]

        # Calcular dias em atraso
        dias_atraso = int(cliente[5]) if cliente[5] else 0

        # Calcular dias desde última compra
        dias_ultima_compra = 0
        if cliente[6]:
            dias_ultima_compra = (datetime.now() - cliente[6]).days

        # Calcular saldo de crédito
        saldo_credito = cliente[2] - cliente[3]  # Limite - Débito

        logger.info(f"✅ Análise do cliente {cliente_id} gerada com sucesso")

        return render_template('analise_cliente.html',
                             cliente=cliente,
                             produtos=produtos_abc,
                             comparativo=comparativo_lista,
                             dias_atraso=dias_atraso,
                             dias_ultima_compra=dias_ultima_compra,
                             saldo_credito=saldo_credito,
                             total_vendas=total_vendas)

    except Exception as e:
        logger.error(f"❌ Erro na análise: {e}\n{traceback.format_exc()}")
        return render_template('404.html'), 404

# ============================================
# ROTAS - ARQUIVOS ESTÁTICOS
# ============================================

@app.route('/favicon.ico')
def favicon():
    """Retorna favicon"""
    try:
        return send_from_directory(
            os.path.join(app.root_path, 'static'),
            'favicon.ico',
            mimetype='image/vnd.microsoft.icon'
        )
    except:
        return '', 204

@app.route('/static/<path:filename>')
def static_files(filename):
    """Serve arquivos estáticos"""
    return send_from_directory(os.path.join(app.root_path, 'static'), filename)

# ============================================
# TRATAMENTO DE ERROS
# ============================================

@app.errorhandler(404)
def not_found(error):
    """Página 404"""
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    """Página 500"""
    logger.error(f"❌ Erro 500: {error}")
    return render_template('500.html'), 500

# ============================================
# INICIALIZAÇÃO
# ============================================

if __name__ == '__main__':
    load_config()
    connect_to_database()
    logger.info("✅ Conectado automaticamente ao banco")
    app.run(host='0.0.0.0', port=5000, debug=True)
