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
app.config['SECRET_KEY'] = 'varejao_bi_farma_2026_final_v11'
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)

EXCEL_PATH = r'C:\Projeto_Varejao\bi_flask_app\database\Vlr_ObjetivoClie.xlsx'

# ============================================
# CONEXÃO E UTILITÁRIOS
# ============================================

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
        logger.error(f"❌ Erro SQL: {e}")
        return []

def get_objetivos_excel():
    if os.path.exists(EXCEL_PATH):
        try:
            df = pd.read_excel(EXCEL_PATH)
            df['Codigo'] = df['Codigo'].astype(int)
            return df.set_index('Codigo')['Vlr_ObjetivoClie'].to_dict()
        except: return {}
    return {}

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session: return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# ============================================
# ROTAS
# ============================================

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
    vendedores = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado IN (0, '0') ORDER BY Nome_guerra")
    cal = {'uteis': 21, 'trabalhados': 13, 'restantes': 8}

    # --- 1. INDICADORES DA EMPRESA (VEOBJ) ---
    if filtro == 'vendedor' and valor:
        res_m = execute_query(f"SELECT ISNULL(Vlr_Cota, 0) FROM VEOBJ WHERE Cod_Vendedor = {int(valor)} AND Ano_Ref = 2026 AND Mes_Ref = 1")
        meta_emp = float(res_m[0][0]) if res_m else 0.0
        query_v = f"SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WITH (NOLOCK) WHERE Cod_Vendedor = {int(valor)} AND Status = 'F' AND MONTH(Dat_Emissao) = 1 AND YEAR(Dat_Emissao) = 2026"
        realizado_emp = float(execute_query(query_v)[0][0] or 0)
        titulo_vendas = f"Vendas: {next((v[1] for v in vendedores if str(v[0])==valor), 'Vendedor')}"
    else:
        res_m = execute_query("SELECT ISNULL(SUM(Vlr_Cota), 0) FROM VEOBJ WHERE Ano_Ref = 2026 AND Mes_Ref = 1")
        meta_emp = float(res_m[0][0]) if res_m else 0.0
        realizado_emp = float(execute_query("SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WITH (NOLOCK) WHERE Status = 'F' AND MONTH(Dat_Emissao) = 1 AND YEAR(Dat_Emissao) = 2026")[0][0] or 0)
        titulo_vendas = "Vendas Gerais (Toda a Empresa)"

    v_proj_emp = (realizado_emp / cal['trabalhados']) * cal['uteis'] if cal['trabalhados'] > 0 else 0
    ating_emp = (v_proj_emp / meta_emp * 100) if meta_emp > 0 else 0
    cor_emp = "#f5576c" if ating_emp < 90 else "#ff9800" if ating_emp < 100 else "#4caf50"

    # --- 2. LISTA DE CLIENTES E CÁLCULOS GERAIS ---
    query_clie = """
    SELECT cl.Codigo, cl.Razao_Social, ISNULL(cl.Limite_Credito, 0), ISNULL(cl.Total_Debito, 0), 
    ISNULL((SELECT MAX(DATEDIFF(DAY, DATEADD(DAY, ISNULL(Qtd_DiaExtVct, 0), Dat_Vencimento), GETDATE()))
            FROM CTREC WITH (NOLOCK) WHERE Cod_Cliente = cl.Codigo AND Vlr_Saldo > 0 AND Status IN ('A', 'P') AND Dat_Vencimento < GETDATE()), 0) AS Atraso,
    (SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WITH (NOLOCK) WHERE Cod_Cliente = cl.Codigo AND Status = 'F' AND MONTH(Dat_Emissao) = 1 AND YEAR(Dat_Emissao) = 2026)
    FROM clien cl WITH (NOLOCK)
    LEFT JOIN enxes en ON cl.Codigo = en.Cod_Client AND en.Cod_Estabe = 0
    WHERE cl.Bloqueado IN (0, '0') """

    if filtro == 'vendedor' and valor: query_clie += f" AND en.Cod_Vendedor = {int(valor)}"
    elif filtro == 'cliente' and valor: query_clie += f" AND (cl.Codigo LIKE '%{valor}%' OR cl.Razao_Social LIKE '%{valor}%')"

    res_db = execute_query(query_clie + " ORDER BY cl.Razao_Social")
    obj_excel = get_objetivos_excel()
    
    clientes_finais, total_meta_clie, total_venda_clie = [], 0, 0
    total_limite, total_debito, qtd_atraso = 0, 0, 0

    for r in res_db:
        lim, deb, atraso, venda = float(r[2] or 0), float(r[3] or 0), int(r[4] or 0), float(r[5] or 0)
        if deb <= 0: atraso = 0
        
        meta_c = obj_excel.get(r[0], 0)
        total_meta_clie += meta_c
        total_venda_clie += venda
        total_limite += lim
        total_debito += deb
        if atraso > 0: qtd_atraso += 1
        
        ating = (venda / meta_c * 100) if meta_c > 0 else 0
        clientes_finais.append([r[0], r[1], 'Não', lim, deb, 0, atraso, '', '', 0, venda, meta_c, ating])

    v_proj_clie = (total_venda_clie / cal['trabalhados']) * cal['uteis'] if cal['trabalhados'] > 0 else 0
    ating_clie = (v_proj_clie / total_meta_clie * 100) if total_meta_clie > 0 else 0
    cor_clie = "#f5576c" if ating_clie < 90 else "#ff9800" if ating_clie < 100 else "#4caf50"

    return render_template('dashboard.html', 
                         clientes=clientes_finais, vendedores=vendedores, filtro_ativo=filtro, valor_filtro=valor,
                         proj={'meta': meta_emp, 'realizado': realizado_emp, 'valor_projecao': v_proj_emp, 'atingimento_proj': ating_emp, 'cor': cor_emp, 'titulo': titulo_vendas},
                         clie_proj={'meta': total_meta_clie, 'realizado': total_venda_clie, 'valor_projecao': v_proj_clie, 'atingimento_proj': ating_clie, 'cor': cor_clie},
                         geral_clie={'limite': total_limite, 'debito': total_debito, 'atraso': qtd_atraso})

@app.route('/analise/<int:cliente_id>')
@login_required
def analise_cliente(cliente_id):
    res = execute_query(f"SELECT Codigo, Razao_Social, ISNULL(Limite_Credito, 0), ISNULL(Total_Debito, 0) FROM clien WITH (NOLOCK) WHERE Codigo = {cliente_id}")
    if not res: return redirect(url_for('dashboard'))
    
    query_titulos = f"SELECT Num_Documento, Par_Documento, Vlr_Documento, Vlr_Saldo, Dat_Emissao, Dat_Vencimento, DATEDIFF(DAY, DATEADD(DAY, ISNULL(Qtd_DiaExtVct, 0), Dat_Vencimento), GETDATE()) FROM CTREC WITH (NOLOCK) WHERE Cod_Cliente = {cliente_id} AND Cod_Estabe = 0 AND Vlr_Saldo > 0 AND Status IN ('A', 'P') ORDER BY Dat_Vencimento ASC"
    titulos = execute_query(query_titulos)
    atraso_max = max([int(t[6]) for t in titulos if int(t[6]) > 0] or [0])
    
    obj = get_objetivos_excel().get(cliente_id, 0)
    venda = float(execute_query(f"SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WITH (NOLOCK) WHERE Cod_Cliente = {cliente_id} AND Status = 'F' AND MONTH(Dat_Emissao) = 1 AND YEAR(Dat_Emissao) = 2026")[0][0] or 0)
    
    raw = execute_query(f"SELECT MONTH(Dat_Emissao), YEAR(Dat_Emissao), SUM(Vlr_TotalNota) FROM NFSCB WITH (NOLOCK) WHERE Cod_Cliente = {cliente_id} AND Status = 'F' AND YEAR(Dat_Emissao) IN (2024, 2025, 2026) GROUP BY MONTH(Dat_Emissao), YEAR(Dat_Emissao) ORDER BY 1, 2")
    meses = ['JAN','FEV','MAR','ABR','MAI','JUN','JUL','AGO','SET','OUT','NOV','DEZ']
    comp = {i: {'mes': meses[i-1], '2024': 0, '2025': 0, '2026': 0} for i in range(1, 13)}
    for r in raw: comp[r[0]][str(r[1])] = float(r[2] or 0)

    return render_template('analise_cliente.html', cliente=res[0], comparativo=list(comp.values()), limite_credito=float(res[0][2]), saldo=float(res[0][2]-res[0][3]), dias_atraso=atraso_max, objetivo=obj, vendas_atual=venda, titulos=titulos)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)