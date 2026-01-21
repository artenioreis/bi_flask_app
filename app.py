from flask import Flask, render_template, request, session, redirect, url_for
from flask_session import Session
import pyodbc
import json
import os
import logging
import pandas as pd
from datetime import datetime
from functools import wraps

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'varejao_bi_2026'
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)

EXCEL_PATH = r'C:\Projeto_Varejao\bi_flask_app\database\Vlr_ObjetivoClie.xlsx'

def execute_query(query):
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
        logger.error(f"Erro SQL: {e}")
        return []

def get_objetivos_excel():
    try:
        if os.path.exists(EXCEL_PATH):
            df = pd.read_excel(EXCEL_PATH)
            df['Codigo'] = df['Codigo'].astype(int)
            return df.set_index('Codigo')['Vlr_ObjetivoClie'].to_dict()
    except Exception as e:
        logger.error(f"Erro ao ler Excel: {e}")
    return {}

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user' not in session: return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

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

    if not valor and filtro != 'todos':
        return render_template('dashboard.html', clientes=[], vendedores=vendedores, filtro_ativo=filtro, valor_filtro='')

    # Query Ajustada: Filtra apenas vendas do MÊS ATUAL para comparação com o objetivo
    query = """
    SELECT cl.Codigo, cl.Razao_Social, 
    CASE WHEN cl.Bloqueado = '0' THEN 'Não' ELSE 'Sim' END,
    ISNULL(cl.Limite_Credito, 0), ISNULL(cl.Total_Debito, 0), 0,
    ISNULL(cl.Maior_Atraso, 0), cl.Data_UltimaFatura, ISNULL(ve.Nome_guerra, 'N/A'), 0,
    (SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB 
     WHERE Cod_Cliente = cl.Codigo AND Status = 'F' 
     AND MONTH(Dat_Emissao) = MONTH(GETDATE()) 
     AND YEAR(Dat_Emissao) = YEAR(GETDATE()))
    FROM clien cl
    LEFT JOIN enxes en ON cl.Codigo = en.Cod_Client AND en.Cod_Estabe = 0
    LEFT JOIN vende ve ON en.Cod_Vendedor = ve.Codigo
    WHERE cl.Bloqueado = '0' """
    
    if filtro == 'vendedor' and valor: query += f" AND en.Cod_Vendedor = {int(valor)}"
    if filtro == 'cliente' and valor: query += f" AND (cl.Codigo LIKE '%{valor}%' OR cl.Razao_Social LIKE '%{valor}%')"
    
    res_db = execute_query(query + " ORDER BY cl.Razao_Social")
    objetivos = get_objetivos_excel()
    
    clientes_finais = []
    for r in res_db:
        cliente_lista = list(r)
        cod = cliente_lista[0]
        vendas_mes = float(cliente_lista[10])
        obj = objetivos.get(cod, 0)
        cliente_lista.append(obj) # Meta Mensal (Index 11)
        atingimento = (vendas_mes / obj * 100) if obj > 0 else 0
        cliente_lista.append(atingimento) # % Atingimento (Index 12)
        clientes_finais.append(cliente_lista)

    return render_template('dashboard.html', clientes=clientes_finais, vendedores=vendedores, filtro_ativo=filtro, valor_filtro=valor)

@app.route('/analise/<int:cliente_id>')
@login_required
def analise_cliente(cliente_id):
    res = execute_query(f"SELECT Codigo, Razao_Social, ISNULL(Limite_Credito, 0), ISNULL(Total_Debito, 0), 0, ISNULL(Maior_Atraso, 0) FROM clien WHERE Codigo = {cliente_id}")
    if not res: return redirect(url_for('dashboard'))
    cliente = res[0]
    
    objetivos = get_objetivos_excel()
    obj_valor = objetivos.get(cliente_id, 0)
    
    # Busca vendas do Mês Atual para o card de análise
    vendas_res = execute_query(f"""
        SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB 
        WHERE Cod_Cliente = {cliente_id} AND Status = 'F' 
        AND MONTH(Dat_Emissao) = MONTH(GETDATE()) 
        AND YEAR(Dat_Emissao) = YEAR(GETDATE())""")
    vendas_mes = float(vendas_res[0][0]) if vendas_res else 0.0

    raw_hist = execute_query(f"""
        SELECT MONTH(Dat_Emissao), YEAR(Dat_Emissao), SUM(Vlr_TotalNota) 
        FROM NFSCB WHERE Cod_Cliente = {cliente_id} AND Status = 'F' AND YEAR(Dat_Emissao) IN (2024, 2025, 2026) 
        GROUP BY MONTH(Dat_Emissao), YEAR(Dat_Emissao) ORDER BY 1, 2""")
    
    meses = ['JAN','FEV','MAR','ABR','MAI','JUN','JUL','AGO','SET','OUT','NOV','DEZ']
    comparativo = {i: {'mes': meses[i-1], '2024': 0, '2025': 0, '2026': 0} for i in range(1, 13)}
    for r in raw_hist: comparativo[r[0]][str(r[1])] = float(r[2])

    return render_template('analise_cliente.html', 
                         cliente=cliente, 
                         comparativo=list(comparativo.values()), 
                         limite_credito=float(cliente[2]), 
                         saldo=float(cliente[2]-cliente[3]), 
                         dias_atraso=int(cliente[5]),
                         objetivo=obj_valor,
                         vendas_atual=vendas_mes)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)