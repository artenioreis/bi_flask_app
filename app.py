from flask import Flask, render_template, request, session, redirect, url_for
from flask_session import Session
import pyodbc
import json
import os
import logging
import pandas as pd
from datetime import datetime
from functools import wraps

# Configuração de logging para monitoramento de erros no console
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'varejao_bi_farma_2026_final_consolidado'
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)

# Caminho para o arquivo Excel de metas dos clientes
EXCEL_PATH = r'C:\Projeto_Varejao\bi_flask_app\database\Vlr_ObjetivoClie.xlsx'

# ============================================
# CONEXÃO E UTILITÁRIOS
# ============================================

def execute_query(query):
    """Conecta ao SQL Server e executa a consulta de forma segura."""
    try:
        config_path = os.path.join(app.root_path, 'database', 'config.json')
        with open(config_path, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
        
        conn_str = (f"Driver={{ODBC Driver 17 for SQL Server}};Server={cfg['server']};"
                    f"Database={cfg['database']};UID={cfg['username']};PWD={cfg['password']};")
        
        conn = pyodbc.connect(conn_str, timeout=15)
        cursor = conn.cursor()
        cursor.execute(query)
        res = cursor.fetchall()
        conn.close()
        return res
    except Exception as e:
        logger.error(f"❌ Erro SQL: {e}")
        return []

def get_objetivos_excel():
    """Lê as metas dos clientes do arquivo Excel."""
    if os.path.exists(EXCEL_PATH):
        try:
            df = pd.read_excel(EXCEL_PATH)
            df['Codigo'] = df['Codigo'].astype(int)
            return df.set_index('Codigo')['Vlr_ObjetivoClie'].to_dict()
        except Exception as e:
            logger.error(f"❌ Erro ao ler Excel: {e}")
            return {}
    return {}

def login_required(f):
    """Garante proteção de rotas para usuários logados."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# ============================================
# ROTAS DO SISTEMA
# ============================================

@app.route('/')
def index():
    if 'user' in session:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        # Simulação de login - armazena o usuário na sessão
        session['user'] = request.form.get('username')
        return redirect(url_for('dashboard'))
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    filtro = request.args.get('tipo', 'todos')
    valor = request.args.get('valor', '').strip()
    
    # Lista de vendedores ativos para o seletor de filtros
    vendedores = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado IN (0, '0') ORDER BY Nome_guerra")
    
    # Calendário de Vendas (Janeiro 2026)
    cal = {'uteis': 21, 'trabalhados': 13, 'restantes': 8}

    # --- 1. INDICADORES GERAIS DA EMPRESA (VENDAS) ---
    if filtro == 'vendedor' and valor:
        res_m = execute_query(f"SELECT ISNULL(Vlr_Cota, 0) FROM VEOBJ WHERE Cod_Vendedor = {int(valor)} AND Ano_Ref = 2026 AND Mes_Ref = 1")
        meta = float(res_m[0][0]) if res_m else 0.0
        query_venda = f"SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WITH (NOLOCK) WHERE Cod_Vendedor = {int(valor)} AND Status = 'F' AND MONTH(Dat_Emissao) = 1 AND YEAR(Dat_Emissao) = 2026"
        realizado = float(execute_query(query_venda)[0][0] or 0)
        titulo_vendas = f"Vendas: {next((v[1] for v in vendedores if str(v[0])==valor), 'Vendedor')}"
    else:
        # Padrão: Soma de toda a empresa
        res_m = execute_query("SELECT ISNULL(SUM(Vlr_Cota), 0) FROM VEOBJ WHERE Ano_Ref = 2026 AND Mes_Ref = 1")
        meta = float(res_m[0][0]) if res_m else 0.0
        query_total = "SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WITH (NOLOCK) WHERE Status = 'F' AND MONTH(Dat_Emissao) = 1 AND YEAR(Dat_Emissao) = 2026"
        realizado = float(execute_query(query_total)[0][0] or 0)
        titulo_vendas = "Vendas Gerais (Toda a Empresa)"

    v_proj = (realizado / cal['trabalhados']) * cal['uteis'] if cal['trabalhados'] > 0 else 0
    ating_proj = (v_proj / meta * 100) if meta > 0 else 0
    cor_vendas = "#f5576c" if ating_proj < 90 else "#ff9800" if ating_proj < 100 else "#4caf50"

    # --- 2. LISTA DE CLIENTES E INDICADORES GERAIS DE CLIENTE (FINANCEIRO) ---
    query_clie = """
    SELECT cl.Codigo, cl.Razao_Social, CASE WHEN cl.Bloqueado IN (0, '0') THEN 'Não' ELSE 'Sim' END,
    ISNULL(cl.Limite_Credito, 0), ISNULL(cl.Total_Debito, 0), 0, 
    ISNULL((SELECT MAX(DATEDIFF(DAY, DATEADD(DAY, ISNULL(Qtd_DiaExtVct, 0), Dat_Vencimento), GETDATE()))
            FROM CTREC WITH (NOLOCK) WHERE Cod_Cliente = cl.Codigo AND Cod_Estabe = 0 AND Vlr_Saldo > 0 
            AND Status IN ('A', 'P') AND Dat_Vencimento < GETDATE()), 0) AS Atraso_Real,
    cl.Data_UltimaFatura, ISNULL(ve.Nome_guerra, 'N/A'), 0,
    (SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WITH (NOLOCK) WHERE Cod_Cliente = cl.Codigo AND Status = 'F' AND MONTH(Dat_Emissao) = 1 AND YEAR(Dat_Emissao) = 2026)
    FROM clien cl WITH (NOLOCK)
    LEFT JOIN enxes en ON cl.Codigo = en.Cod_Client AND en.Cod_Estabe = 0
    LEFT JOIN vende ve ON en.Cod_Vendedor = ve.Codigo
    WHERE cl.Bloqueado IN (0, '0') """

    if filtro == 'vendedor' and valor:
        query_clie += f" AND en.Cod_Vendedor = {int(valor)}"
    elif filtro == 'cliente' and valor:
        query_clie += f" AND (cl.Codigo LIKE '%{valor}%' OR cl.Razao_Social LIKE '%{valor}%')"

    res_db = execute_query(query_clie + " ORDER BY cl.Razao_Social")
    obj_excel = get_objetivos_excel()
    
    clientes_finais = []
    total_limite = 0
    total_debito = 0
    qtd_atraso = 0

    for r in res_db:
        c = list(r)
        atraso_real = int(c[6])
        if float(c[4]) <= 0: atraso_real = 0 # Força 0 se não houver débito
        
        # Acumular indicadores gerais de cliente
        total_limite += float(c[3])
        total_debito += float(c[4])
        if atraso_real > 0: qtd_atraso += 1
        
        meta_c = obj_excel.get(c[0], 0)
        ating_clie = (float(c[10]) / meta_c * 100) if meta_c > 0 else 0
        c[6] = atraso_real
        c.extend([meta_c, ating_clie])
        clientes_finais.append(c)

    return render_template('dashboard.html', 
                         clientes=clientes_finais, 
                         vendedores=vendedores, 
                         filtro_ativo=filtro, 
                         valor_filtro=valor, 
                         proj={'meta': meta, 'realizado': realizado, 'valor_projecao': v_proj, 'atingimento_proj': ating_proj, 'cor': cor_vendas, 'titulo': titulo_vendas},
                         clie_geral={'limite': total_limite, 'debito': total_debito, 'atrasados': qtd_atraso})

@app.route('/analise/<int:cliente_id>')
@login_required
def analise_cliente(cliente_id):
    # Consulta básica do cliente
    res = execute_query(f"SELECT Codigo, Razao_Social, ISNULL(Limite_Credito, 0), ISNULL(Total_Debito, 0), 0 FROM clien WITH (NOLOCK) WHERE Codigo = {cliente_id}")
    if not res: return redirect(url_for('dashboard'))
    cliente = res[0]

    # Busca os boletos (títulos) em aberto na CTREC
    query_titulos = f"""
    SELECT Num_Documento, Par_Documento, Vlr_Documento, Vlr_Saldo, Dat_Emissao, Dat_Vencimento,
    DATEDIFF(DAY, DATEADD(DAY, ISNULL(Qtd_DiaExtVct, 0), Dat_Vencimento), GETDATE()) AS DiasAtraso
    FROM CTREC WITH (NOLOCK) WHERE Cod_Cliente = {cliente_id} AND Cod_Estabe = 0 AND Vlr_Saldo > 0 AND Status IN ('A', 'P')
    ORDER BY Dat_Vencimento ASC"""
    
    titulos = execute_query(query_titulos)
    maior_atraso = max([int(t[6]) for t in titulos if int(t[6]) > 0] or [0])

    # Objetivo (Meta) do Excel
    objetivo = get_objetivos_excel().get(cliente_id, 0)
    
    # Realizado no Mês
    vendas_mes = float(execute_query(f"SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WITH (NOLOCK) WHERE Cod_Cliente = {cliente_id} AND Status = 'F' AND MONTH(Dat_Emissao) = 1 AND YEAR(Dat_Emissao) = 2026")[0][0] or 0)
    
    # Histórico de Vendas Trienal
    raw_hist = execute_query(f"SELECT MONTH(Dat_Emissao), YEAR(Dat_Emissao), SUM(Vlr_TotalNota) FROM NFSCB WITH (NOLOCK) WHERE Cod_Cliente = {cliente_id} AND Status = 'F' AND YEAR(Dat_Emissao) IN (2024, 2025, 2026) GROUP BY MONTH(Dat_Emissao), YEAR(Dat_Emissao) ORDER BY 1, 2")
    meses = ['JAN','FEV','MAR','ABR','MAI','JUN','JUL','AGO','SET','OUT','NOV','DEZ']
    comp = {i: {'mes': meses[i-1], '2024': 0, '2025': 0, '2026': 0} for i in range(1, 13)}
    for r in raw_hist: comp[r[0]][str(r[1])] = float(r[2] or 0)

    return render_template('analise_cliente.html', 
                         cliente=cliente, 
                         comparativo=list(comp.values()), 
                         limite_credito=float(cliente[2]), 
                         saldo=float(cliente[2]-cliente[3]), 
                         dias_atraso=maior_atraso, 
                         objetivo=objetivo, 
                         vendas_atual=vendas_mes, 
                         titulos=titulos)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)