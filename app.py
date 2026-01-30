from flask import Flask, render_template, request, session, redirect, url_for, flash, jsonify
from flask_session import Session
import pyodbc
import json
import os
import logging
import pandas as pd
from datetime import datetime, date
from functools import wraps

# Configuração de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
# Identificação da Versão Blindada v43.4
app.config['SECRET_KEY'] = 'varejao_bi_farma_2026_v43_4_blindada'
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)

CONFIG_PATH = os.path.join(app.root_path, 'database', 'config.json')
EXCEL_PATH = r'C:\Projeto_Varejao\bi_flask_app\database\Vlr_ObjetivoClie.xlsx'

# ============================================
# NÚCLEO TÉCNICO SQL
# ============================================

def execute_query(query):
    try:
        if not os.path.exists(CONFIG_PATH): return []
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
        conn_str = (f"Driver={{ODBC Driver 17 for SQL Server}};Server={cfg['server']};"
                    f"Database={cfg['database']};UID={cfg['username']};PWD={cfg['password']};")
        conn = pyodbc.connect(conn_str, timeout=10)
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
# ACESSO E SEGURANÇA
# ============================================

@app.route('/')
def index():
    return redirect(url_for('dashboard')) if 'user' in session else redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        user = request.form.get('username', '').strip()
        pwd = request.form.get('password', '').strip()
        if user == 'admin' and pwd == 'admin123456':
            session['user'] = user
            return redirect(url_for('dashboard'))
        return render_template('login.html', erro="Acesso Negado!", config=get_db_cfg())
    return render_template('login.html', config=get_db_cfg())

def get_db_cfg():
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f: return json.load(f)
        except: pass
    return {}

@app.route('/configurar_banco', methods=['POST'])
def configurar_banco():
    dados = {k: request.form.get(k) for k in ['server', 'database', 'username', 'password']}
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f: json.dump(dados, f, indent=4)
    return redirect(url_for('login'))

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

# ============================================
# DASHBOARD v43.4
# ============================================

@app.route('/dashboard')
@login_required
def dashboard():
    filtro = request.args.get('tipo', 'todos')
    valor = request.args.get('valor', '').strip()
    v_list = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado IN (0, '0') ORDER BY Nome_guerra")
    cal = {'uteis': 21, 'trabalhados': 13}
    hoje = date.today()
    obj_ex = get_objetivos_excel()

    r_cia = float(execute_query("SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WITH (NOLOCK) WHERE Status = 'F' AND MONTH(Dat_Emissao) = 1")[0][0] or 0)
    m_cia = float(execute_query("SELECT ISNULL(SUM(Vlr_Cota), 0) FROM VEOBJ WHERE Ano_Ref = 2026 AND Mes_Ref = 1")[0][0] or 1)
    
    m_sel, r_sel, p_sel, a_sel = 0, 0, 0, 0
    v_stats = {'total_carteira': 0, 'atendidos': 0}
    if filtro == 'vendedor' and valor:
        m_sel = float(execute_query(f"SELECT ISNULL(SUM(Vlr_Cota), 0) FROM VEOBJ WHERE Cod_Vendedor = {int(valor)} AND Ano_Ref = 2026 AND Mes_Ref = 1")[0][0] or 1)
        r_sel = float(execute_query(f"SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WHERE Cod_Vendedor = {int(valor)} AND Status = 'F' AND MONTH(Dat_Emissao) = 1")[0][0] or 0)
        p_sel = (r_sel / cal['trabalhados'] * cal['uteis'])
        a_sel = (p_sel / m_sel * 100)
        v_stats['total_carteira'] = int(execute_query(f"SELECT COUNT(DISTINCT Cod_Client) FROM enxes WHERE Cod_Vendedor = {int(valor)} AND Cod_Estabe = 0")[0][0] or 0)
        v_stats['atendidos'] = int(execute_query(f"SELECT COUNT(DISTINCT Cod_Cliente) FROM NFSCB WHERE Cod_Vendedor = {int(valor)} AND Status = 'F' AND MONTH(Dat_Emissao) = 1")[0][0] or 0)

    clientes_finais, t_m_c, t_v_c, t_lim, t_deb, q_atr = [], 0, 0, 0, 0, 0
    query_clie = """SELECT cl.Codigo, cl.Razao_Social, ISNULL(cl.Limite_Credito, 0), ISNULL(cl.Total_Debito, 0), 
    (SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WHERE Cod_Cliente = cl.Codigo AND Status = 'F' AND MONTH(Dat_Emissao) = 1) as Vnd
    FROM clien cl INNER JOIN enxes en ON cl.Codigo = en.Cod_Client AND en.Cod_Estabe = 0 WHERE cl.Bloqueado = 0"""
    
    if filtro == 'vendedor' and valor: query_clie += f" AND en.Cod_Vendedor = {int(valor)}"
    elif filtro == 'cliente' and valor: query_clie += f" AND (cl.Codigo LIKE '%{valor}%' OR cl.Razao_Social LIKE '%{valor}%')"

    res_db = execute_query(query_clie)
    for r in res_db:
        m_c = obj_ex.get(r[0], 0)
        vnd = float(r[4] or 0)
        t_m_c += m_c; t_v_c += vnd; t_lim += float(r[2]); t_deb += float(r[3])
        sql_at = f"SELECT MIN(Dat_Vencimento) FROM CTREC WHERE Cod_Cliente = {r[0]} AND Vlr_Saldo > 0 AND Status IN ('A', 'P')"
        res_at = execute_query(sql_at)
        atr_d = 0
        if res_at and res_at[0][0]:
            venc = res_at[0][0].date() if isinstance(res_at[0][0], datetime) else res_at[0][0]
            if venc < hoje: atr_d = (hoje - venc).days; q_atr += 1
        if valor or len(res_db) < 150:
            clientes_finais.append([r[0], r[1], 'Não', float(r[2]), float(r[3]), 0, atr_d, '', '', 0, vnd, m_c, (vnd/m_c*100 if m_c>0 else 0)])

    return render_template('dashboard.html', clientes=clientes_finais, vendedores=v_list, filtro_ativo=filtro, valor_filtro=valor,
                         proj={'meta': m_cia, 'realizado': r_cia, 'valor_projecao': (r_cia/cal['trabalhados']*cal['uteis']), 'atingimento_proj': (r_cia/m_cia*100), 'titulo': "VENDAS EMPRESA"},
                         sel={'meta': m_sel, 'realizado': r_sel, 'valor_projecao': p_sel, 'atingimento_proj': a_sel},
                         clie_proj={'meta': t_m_c, 'realizado': t_v_c, 'valor_projecao': (t_v_c/cal['trabalhados']*cal['uteis']), 'atingimento_proj': (t_v_c/t_m_c*100 if t_m_c>0 else 0)},
                         geral_clie={'limite': t_lim, 'debito': t_deb, 'atraso': q_atr}, vendedor_stats=v_stats)

# ============================================
# ANÁLISE CLIENTE (GRÁFICO 2024-2026)
# ============================================

@app.route('/analise/<int:cliente_id>')
@login_required
def analise_cliente(cliente_id):
    res = execute_query(f"SELECT Codigo, Razao_Social, ISNULL(Limite_Credito, 0), ISNULL(Total_Debito, 0) FROM clien WHERE Codigo = {cliente_id}")
    if not res: return redirect(url_for('dashboard'))
    
    titulos = execute_query(f"SELECT Num_Documento, Par_Documento, Vlr_Documento, Vlr_Saldo, Dat_Emissao, Dat_Vencimento, DATEDIFF(DAY, Dat_Vencimento, GETDATE()) FROM CTREC WHERE Cod_Cliente = {cliente_id} AND Vlr_Saldo > 0")
    d_atr_max = max([int(t[6]) for t in titulos if int(t[6]) > 0] or [0])
    v_at = float(execute_query(f"SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WHERE Cod_Cliente = {cliente_id} AND Status = 'F' AND MONTH(Dat_Emissao) = 1")[0][0] or 0)
    
    # Busca histórica obrigatória
    sql_hist = f"SELECT YEAR(Dat_Emissao), MONTH(Dat_Emissao), SUM(Vlr_TotalNota) FROM NFSCB WITH (NOLOCK) WHERE Cod_Cliente = {cliente_id} AND Status = 'F' AND Cod_Estabe = 0 AND YEAR(Dat_Emissao) IN (2024, 2025, 2026) GROUP BY YEAR(Dat_Emissao), MONTH(Dat_Emissao) ORDER BY 1, 2"
    res_hist = execute_query(sql_hist)
    comparativo_data = [{'ano': int(h[0]), 'mes': int(h[1]), 'total': float(h[2])} for h in res_hist]
    
    return render_template('analise_cliente.html', cliente=res[0], limite_credito=float(res[0][2]), saldo=float(res[0][2]-res[0][3]), dias_atraso=d_atr_max, comparativo=comparativo_data, objetivo=get_objetivos_excel().get(cliente_id, 0), vendas_atual=v_at, titulos=titulos)

# ============================================
# MAPA REGIONAL (FIX Erro 22007)
# ============================================

@app.route('/mapa')
@login_required
def mapa_vendas():
    inicio_raw = request.args.get('inicio', '2026-01-01')
    fim_raw = request.args.get('fim', '2026-01-31')
    vendedor_id = request.args.get('vendedor', '')
    
    v_list = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado IN (0, '0') ORDER BY Nome_guerra")
    regioes, chart_ml, stats = {}, [], {'movel_qtd': 0, 'movel_vlr': 0.0, 'eletro_qtd': 0, 'eletro_vlr': 0.0, 'total_qtd': 0, 'total_vlr': 0.0, 'clientes_atendidos': 0, 'operadores': {}}

    if vendedor_id:
        # Formato compactado para evitar erro smalldatetime
        d_ini = inicio_raw.replace("-", "")
        d_fim = fim_raw.replace("-", "")

        query = f"""SELECT ISNULL(nf.Cidade, 'NAO INF.'), ISNULL(nf.Bairro, 'NAO INF.'), nf.Cod_OrigemNfs, 
                    SUM(nf.Vlr_TotalNota), COUNT(nf.Num_Nota), ISNULL(ve.Nome_Guerra, 'NAO IDENT.') 
                    FROM nfscb nf WITH (NOLOCK) LEFT JOIN VENDE ve ON ve.Codigo = nf.Cod_VendTlmkt
                    WHERE nf.Cod_Estabe = 0 AND nf.Status = 'F' AND nf.Cod_Vendedor = {int(vendedor_id)} 
                    AND nf.Dat_Emissao BETWEEN '{d_ini}' AND '{d_fim} 23:59:59'
                    GROUP BY nf.Cidade, nf.Bairro, nf.Cod_OrigemNfs, ve.Nome_Guerra"""
        
        res = execute_query(query)
        for r in res:
            cid, bai, ori, vlr, qtd, ope = r[0].strip(), r[1].strip(), r[2], float(r[3]), int(r[4]), r[5]
            if ori == 'ML': stats['movel_qtd'] += qtd; stats['movel_vlr'] += vlr; chart_ml.append({'label': f"{cid}-{bai}", 'valor': vlr})
            elif ori == 'TL': stats['eletro_qtd'] += qtd; stats['eletro_vlr'] += vlr
            stats['total_qtd'] += qtd; stats['total_vlr'] += vlr
            stats['operadores'][ope] = stats['operadores'].get(ope, 0) + qtd
            if cid not in regioes: regioes[cid] = {}
            if bai not in regioes[cid]: regioes[cid][bai] = {'ML': [0,0], 'total': 0.0}
            if ori == 'ML': regioes[cid][bai]['ML'][0] += vlr; regioes[cid][bai]['ML'][1] += qtd
            regioes[cid][bai]['total'] += vlr
        
        chart_ml = sorted(chart_ml, key=lambda x: x['valor'], reverse=True)[:10]
        q_clie = f"SELECT COUNT(DISTINCT Cod_Cliente) FROM nfscb WHERE Status='F' AND Cod_Estabe=0 AND Cod_Vendedor={int(vendedor_id)} AND Dat_Emissao BETWEEN '{d_ini}' AND '{d_fim} 23:59:59'"
        res_clie = execute_query(q_clie)
        if res_clie: stats['clientes_atendidos'] = int(res_clie[0][0])

    return render_template('mapa.html', regioes=regioes, vendedores=v_list, chart_ml=chart_ml, data_inicio=inicio_raw, data_fim=fim_raw, vendedor_selecionado=vendedor_id, stats=stats)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)