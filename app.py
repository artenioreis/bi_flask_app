from flask import Flask, render_template, request, session, redirect, url_for
from flask_session import Session
import pyodbc
import json
import os
import logging
import pandas as pd
from datetime import datetime
from functools import wraps

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'varejao_farma_2026_total'
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)

EXCEL_PATH = r'C:\Projeto_Varejao\bi_flask_app\database\Vlr_ObjetivoClie.xlsx'

def execute_query(query):
    try:
        config_path = os.path.join(app.root_path, 'database', 'config.json')
        with open(config_path, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
        conn_str = (f"Driver={{ODBC Driver 17 for SQL Server}};Server={cfg.get('server')};"
                    f"Database={cfg.get('database')};UID={cfg.get('username')};PWD={cfg.get('password')};")
        conn = pyodbc.connect(conn_str, timeout=15)
        cursor = conn.cursor()
        cursor.execute(query)
        res = cursor.fetchall()
        conn.close()
        return res
    except Exception as e:
        logger.error(f"Erro SQL: {e}")
        return []

def get_objetivos_excel():
    try:
        if os.path.exists(EXCEL_PATH):
            df = pd.read_excel(EXCEL_PATH)
            df['Codigo'] = df['Codigo'].astype(int)
            return df.set_index('Codigo')['Vlr_ObjetivoClie'].to_dict()
    except Exception: return {}
    return {}

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session: return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/')
def index():
    if 'user' in session: return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
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
    vendedores = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado IN ('0', 0) ORDER BY Nome_guerra")

    cal = {'uteis': 21, 'trabalhados': 13, 'restantes': 8}
    proj_vendedor = None

    if not valor and filtro != 'todos':
        return render_template('dashboard.html', clientes=[], vendedores=vendedores, filtro_ativo=filtro, valor_filtro='')

    query_clie = """
    SELECT cl.Codigo, cl.Razao_Social, CASE WHEN cl.Bloqueado = '0' THEN 'Não' ELSE 'Sim' END,
    ISNULL(cl.Limite_Credito, 0), ISNULL(cl.Total_Debito, 0), 0, ISNULL(cl.Maior_Atraso, 0), 
    cl.Data_UltimaFatura, ISNULL(ve.Nome_guerra, 'N/A'), 0,
    (SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WHERE Cod_Cliente = cl.Codigo AND Status = 'F' AND MONTH(Dat_Emissao) = 1 AND YEAR(Dat_Emissao) = 2026)
    FROM clien cl
    LEFT JOIN enxes en ON cl.Codigo = en.Cod_Client AND en.Cod_Estabe = 0
    LEFT JOIN vende ve ON en.Cod_Vendedor = ve.Codigo
    WHERE cl.Bloqueado = '0' """

    if filtro == 'vendedor' and valor:
        cod_v = int(valor)
        query_clie += f" AND en.Cod_Vendedor = {cod_v}"
        meta = float(execute_query(f"SELECT Isnull(Vlr_Cota, 0) FROM VEOBJ WHERE Cod_Vendedor = {cod_v} AND Ano_Ref = 2026 AND Mes_Ref = 1")[0][0] or 0)
        realizado = float(execute_query(f"SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WHERE Cod_Vendedor = {cod_v} AND Status = 'F' AND MONTH(Dat_Emissao) = 1 AND YEAR(Dat_Emissao) = 2026")[0][0])
        valor_proj = (realizado / cal['trabalhados']) * cal['uteis'] if cal['trabalhados'] > 0 else 0
        proj_vendedor = {'meta': meta, 'realizado': realizado, 'valor_projecao': valor_proj,
                         'atingimento': (realizado/meta*100 if meta>0 else 0),
                         'atingimento_proj': (valor_proj/meta*100 if meta>0 else 0), 'dias': cal}
    elif filtro == 'cliente' and valor:
        query_clie += f" AND (cl.Codigo LIKE '%{valor}%' OR cl.Razao_Social LIKE '%{valor}%')"

    res_db = execute_query(query_clie + " ORDER BY cl.Razao_Social")
    obj_excel = get_objetivos_excel()
    clientes_finais = []
    for r in res_db:
        c = list(r)
        if float(c[4]) <= 0: c[6] = 0
        meta_c = obj_excel.get(c[0], 0)
        c.extend([meta_c, (float(c[10])/meta_c*100 if meta_c>0 else 0)])
        clientes_finais.append(c)

    return render_template('dashboard.html', clientes=clientes_finais, vendedores=vendedores, filtro_ativo=filtro, valor_filtro=valor, proj=proj_vendedor)

@app.route('/analise/<int:cliente_id>')
@login_required
def analise_cliente(cliente_id):
    res = execute_query(f"SELECT Codigo, Razao_Social, ISNULL(Limite_Credito, 0), ISNULL(Total_Debito, 0), 0, ISNULL(Maior_Atraso, 0) FROM clien WHERE Codigo = {cliente_id}")
    if not res: return redirect(url_for('dashboard'))
    cliente = res[0]
    dias_atraso = int(cliente[5]) if float(cliente[3]) > 0 else 0
    objetivo = get_objetivos_excel().get(cliente_id, 0)
    vendas_mes = float(execute_query(f"SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WHERE Cod_Cliente = {cliente_id} AND Status = 'F' AND MONTH(Dat_Emissao) = 1 AND YEAR(Dat_Emissao) = 2026")[0][0])
    raw_hist = execute_query(f"SELECT MONTH(Dat_Emissao), YEAR(Dat_Emissao), SUM(Vlr_TotalNota) FROM NFSCB WHERE Cod_Cliente = {cliente_id} AND Status = 'F' AND YEAR(Dat_Emissao) IN (2024, 2025, 2026) GROUP BY MONTH(Dat_Emissao), YEAR(Dat_Emissao) ORDER BY 1, 2")
    meses = ['JAN','FEV','MAR','ABR','MAI','JUN','JUL','AGO','SET','OUT','NOV','DEZ']
    comp = {i: {'mes': meses[i-1], '2024': 0, '2025': 0, '2026': 0} for i in range(1, 13)}
    for r in raw_hist: comp[r[0]][str(r[1])] = float(r[2])
    return render_template('analise_cliente.html', cliente=cliente, comparativo=list(comp.values()), limite_credito=float(cliente[2]), saldo=float(cliente[2]-cliente[3]), dias_atraso=dias_atraso, objetivo=objetivo, vendas_atual=vendas_mes)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)