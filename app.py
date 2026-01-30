from flask import Flask, render_template, request, session, redirect, url_for, flash, jsonify
from flask_session import Session
import pyodbc
import json
import os
import logging
import pandas as pd
from datetime import datetime, date
from functools import wraps

# Configuração de Logs e Caminhos
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'varejao_bi_farma_2026_v43_final_layout'
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)

CONFIG_PATH = os.path.join(app.root_path, 'database', 'config.json')
EXCEL_PATH = r'C:\Projeto_Varejao\bi_flask_app\database\Vlr_ObjetivoClie.xlsx'

# ============================================
# NÚCLEO TÉCNICO E SQL
# ============================================

def execute_query(query):
    try:
        if not os.path.exists(CONFIG_PATH): return []
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
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
# DASHBOARD v43 (LAYOUT BLOCOS)
# ============================================

@app.route('/dashboard')
@login_required
def dashboard():
    filtro = request.args.get('tipo', 'todos')
    valor = request.args.get('valor', '').strip()
    vendedores = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado IN (0, '0') ORDER BY Nome_guerra")
    
    cal = {'uteis': 21, 'trabalhados': 13}
    hoje = date.today()
    obj_ex = get_objetivos_excel()

    # 1. BLOCO EMPRESA (Sempre Total)
    sql_cia_m = "SELECT ISNULL(SUM(Vlr_Cota), 0) FROM VEOBJ WHERE Ano_Ref = 2026 AND Mes_Ref = 1"
    sql_cia_r = "SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WITH (NOLOCK) WHERE Status = 'F' AND MONTH(Dat_Emissao) = 1"
    m_cia = float(execute_query(sql_cia_m)[0][0] or 0)
    r_cia = float(execute_query(sql_cia_r)[0][0] or 0)
    p_cia = (r_cia / cal['trabalhados'] * cal['uteis']) if cal['trabalhados'] > 0 else 0
    ating_cia = (p_cia / m_cia * 100) if m_cia > 0 else 0
    cor_cia = "#27ae60" if ating_cia >= 100 else "#667eea"

    # 2. BLOCO VENDEDOR (Se selecionado)
    m_sel, r_sel, p_sel, ating_sel = 0, 0, 0, 0
    v_stats = {'total_carteira': 0, 'atendidos': 0}
    if filtro == 'vendedor' and valor:
        sql_v_m = f"SELECT ISNULL(SUM(Vlr_Cota), 0) FROM VEOBJ WHERE Cod_Vendedor = {int(valor)} AND Ano_Ref = 2026 AND Mes_Ref = 1"
        sql_v_r = f"SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WHERE Cod_Vendedor = {int(valor)} AND Status = 'F' AND MONTH(Dat_Emissao) = 1"
        m_sel = float(execute_query(sql_v_m)[0][0] or 0)
        r_sel = float(execute_query(sql_v_r)[0][0] or 0)
        p_sel = (r_sel / cal['trabalhados'] * cal['uteis']) if cal['trabalhados'] > 0 else 0
        ating_sel = (p_sel / m_sel * 100) if m_sel > 0 else 0
        # Gestão de Carteira
        res_tot = execute_query(f"SELECT COUNT(DISTINCT Cod_Client) FROM enxes WHERE Cod_Vendedor = {int(valor)} AND Cod_Estabe = 0")
        v_stats['total_carteira'] = int(res_tot[0][0]) if res_tot else 0
        res_ate = execute_query(f"SELECT COUNT(DISTINCT Cod_Cliente) FROM NFSCB WHERE Cod_Vendedor = {int(valor)} AND Status = 'F' AND MONTH(Dat_Emissao) = 1")
        v_stats['atendidos'] = int(res_ate[0][0]) if res_ate else 0

    # 3. BLOCO CLIENTES (EXCEL) E FILTRO INNER JOIN
    clientes_finais, t_meta_c, t_venda_c, t_lim, t_deb, q_atr = [], 0, 0, 0, 0, 0
    query_clie = """SELECT cl.Codigo, cl.Razao_Social, ISNULL(cl.Limite_Credito, 0), ISNULL(cl.Total_Debito, 0), 
    (SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WHERE Cod_Cliente = cl.Codigo AND Status = 'F' AND MONTH(Dat_Emissao) = 1) as Vnd
    FROM clien cl INNER JOIN enxes en ON cl.Codigo = en.Cod_Client AND en.Cod_Estabe = 0 WHERE cl.Bloqueado = 0"""
    
    if filtro == 'vendedor' and valor: query_clie += f" AND en.Cod_Vendedor = {int(valor)}"
    elif filtro == 'cliente' and valor: query_clie += f" AND (cl.Codigo LIKE '%{valor}%' OR cl.Razao_Social LIKE '%{valor}%')"

    res_db = execute_query(query_clie)
    for r in res_db:
        meta_cli_ex = obj_ex.get(r[0], 0)
        venda = float(r[4] or 0)
        t_meta_c += meta_cli_ex; t_venda_c += venda; t_lim += float(r[2]); t_deb += float(r[3])
        # Atraso Seguro
        sql_at = f"SELECT MIN(Dat_Vencimento) FROM CTREC WHERE Cod_Cliente = {r[0]} AND Vlr_Saldo > 0 AND Status IN ('A', 'P')"
        res_at = execute_query(sql_at)
        atr_d = 0
        if res_at and res_at[0][0]:
            venc = res_at[0][0]
            if isinstance(venc, datetime): venc = venc.date()
            if venc < hoje: atr_d = (hoje - venc).days; q_atr += 1
        ating_c = (venda / meta_cli_ex * 100) if meta_cli_ex > 0 else 0
        if valor or len(res_db) < 100:
            clientes_finais.append([r[0], r[1], 'Não', float(r[2]), float(r[3]), 0, atr_d, '', '', 0, venda, meta_cli_ex, ating_c])

    proj_clie = (t_venda_c / cal['trabalhados'] * cal['uteis']) if cal['trabalhados'] > 0 else 0
    ating_clie = (proj_clie / t_meta_c * 100) if t_meta_c > 0 else 0

    return render_template('dashboard.html', clientes=clientes_finais, vendedores=vendedores, filtro_ativo=filtro, valor_filtro=valor,
                         proj={'meta': m_cia, 'realizado': r_cia, 'valor_projecao': p_cia, 'atingimento_proj': ating_cia, 'cor': cor_cia, 'titulo': "Vendas Empresa"},
                         sel={'meta': m_sel, 'realizado': r_sel, 'valor_projecao': p_sel, 'atingimento_proj': ating_sel, 'cor': "#764ba2", 'titulo': "Performance Vendedor"},
                         clie_proj={'meta': t_meta_c, 'realizado': t_venda_c, 'valor_projecao': proj_clie, 'atingimento_proj': ating_clie, 'cor': "#2ecc71"},
                         geral_clie={'limite': t_lim, 'debito': t_deb, 'atraso': q_atr},
                         vendedor_stats=v_stats)

# --- Rota Mapa v43 Corrigida ---
@app.route('/mapa')
@login_required
def mapa_vendas():
    inicio_raw = request.args.get('inicio', '2026-01-01')
    fim_raw = request.args.get('fim', '2026-01-31')
    vendedor_id = request.args.get('vendedor', '')
    v_list = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado IN (0, '0') ORDER BY Nome_guerra")
    regioes, chart_ml = {}, []
    stats = {'movel_qtd': 0, 'movel_vlr': 0.0, 'eletro_qtd': 0, 'eletro_vlr': 0.0, 'total_qtd': 0, 'total_vlr': 0.0, 'clientes_atendidos': 0, 'operadores': {}}

    if vendedor_id:
        d_ini, d_fim = inicio_raw.replace("-", ""), fim_raw.replace("-", "")
        sql_cli = f"SELECT COUNT(DISTINCT Cod_Cliente) FROM nfscb WHERE Status = 'F' AND Cod_Vendedor = {vendedor_id} AND Dat_Emissao BETWEEN '{d_ini}' AND '{d_fim} 23:59:59'"
        stats['clientes_atendidos'] = int(execute_query(sql_cli)[0][0] or 0)
        query = f"""SELECT ISNULL(nf.Cidade, 'NAO INF.'), ISNULL(nf.Bairro, 'NAO INF.'), nf.Cod_OrigemNfs, SUM(nf.Vlr_TotalNota), COUNT(nf.Num_Nota), ISNULL(ve_tlm.Nome_Guerra, 'NAO IDENT.') 
        FROM nfscb nf WITH (NOLOCK) LEFT JOIN VENDE ve_tlm ON ve_tlm.Codigo = nf.Cod_VendTlmkt
        WHERE nf.Cod_Estabe = 0 AND nf.Status = 'F' AND nf.Cod_Vendedor = {vendedor_id} AND nf.Dat_Emissao BETWEEN '{d_ini}' AND '{d_fim} 23:59:59'
        GROUP BY nf.Cidade, nf.Bairro, nf.Cod_OrigemNfs, ve_tlm.Nome_Guerra"""
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

    return render_template('mapa.html', regioes=regioes, vendedores=v_list, chart_ml=chart_ml, data_inicio=inicio_raw, data_fim=fim_raw, vendedor_selecionado=vendedor_id, stats=stats)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)