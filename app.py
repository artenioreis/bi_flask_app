from flask import Flask, render_template, request, session, redirect, url_for
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
app.config['SECRET_KEY'] = 'varejao_bi_farma_2026_null_fix_v29'
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)

EXCEL_PATH = r'C:\Projeto_Varejao\bi_flask_app\database\Vlr_ObjetivoClie.xlsx'

# ============================================
# UTILITÁRIOS
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
    return redirect(url_for('dashboard')) if 'user' in session else redirect(url_for('login'))

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
    """Dashboard Otimizado: Só carrega clientes após filtro e trata valores NULL."""
    filtro = request.args.get('tipo', 'todos')
    valor = request.args.get('valor', '').strip()
    vendedores = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado = 0 ORDER BY Nome_guerra")
    cal = {'uteis': 21, 'trabalhados': 13}

    # Metas Empresa
    res_m = execute_query("SELECT ISNULL(SUM(Vlr_Cota), 0) FROM VEOBJ WHERE Ano_Ref = 2026 AND Mes_Ref = 1")
    meta_emp = float(res_m[0][0]) if res_m else 0.0
    
    sql_r = "SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WITH (NOLOCK) WHERE Status = 'F' AND MONTH(Dat_Emissao) = 1"
    if filtro == 'vendedor' and valor: sql_r += f" AND Cod_Vendedor = {int(valor)}"
    realizado_emp = float(execute_query(sql_r)[0][0] or 0)
    
    clientes_finais, t_meta_c, t_venda_c, t_lim, t_deb, q_atr = [], 0, 0, 0, 0, 0
    hoje = datetime.now().date()

    if (filtro == 'vendedor' and valor) or (filtro == 'cliente' and valor):
        query_clie = """SELECT cl.Codigo, cl.Razao_Social, cl.Limite_Credito, cl.Total_Debito, 
        (SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WHERE Cod_Cliente = cl.Codigo AND Status = 'F' AND MONTH(Dat_Emissao) = 1) as Vnd
        FROM clien cl WITH (NOLOCK)
        LEFT JOIN enxes en ON cl.Codigo = en.Cod_Client AND en.Cod_Estabe = 0
        WHERE cl.Bloqueado = 0"""
        
        if filtro == 'vendedor': query_clie += f" AND en.Cod_Vendedor = {int(valor)}"
        elif filtro == 'cliente': query_clie += f" AND (cl.Codigo LIKE '%{valor}%' OR cl.Razao_Social LIKE '%{valor}%')"
        
        res_db = execute_query(query_clie + " ORDER BY cl.Razao_Social")
        obj_ex = get_objetivos_excel()

        for r in res_db:
            # CORREÇÃO DO ERRO: Tratando valores nulos (None)
            lim = float(r[2] or 0)
            deb = float(r[3] or 0)
            venda = float(r[4] or 0)
            
            meta_c = obj_ex.get(r[0], 0)
            t_meta_c += meta_c; t_venda_c += venda; t_lim += lim; t_deb += deb
            
            # Cálculo de atraso
            sql_atraso = f"SELECT MIN(Dat_Vencimento) FROM CTREC WHERE Cod_Cliente = {r[0]} AND Vlr_Saldo > 0 AND Status IN ('A', 'P')"
            res_atraso = execute_query(sql_atraso)
            atraso_dias = 0
            if res_atraso and res_atraso[0][0]:
                venc = res_atraso[0][0]
                if isinstance(venc, datetime): venc = venc.date()
                if venc < hoje:
                    atraso_dias = (hoje - venc).days
                    q_atr += 1
            
            clientes_finais.append([r[0], r[1], 'Não', lim, deb, 0, atraso_dias, '', '', 0, venda, meta_c, (venda/meta_c*100 if meta_c>0 else 0)])

    v_proj_clie = (t_venda_c / 13 * 21) if 13 > 0 else 0
    ating_v_geral = (realizado_emp / 13 * 21 / meta_emp * 100) if meta_emp > 0 else 0

    return render_template('dashboard.html', clientes=clientes_finais, vendedores=vendedores, filtro_ativo=filtro, valor_filtro=valor,
                         proj={'meta': meta_emp, 'realizado': realizado_emp, 'valor_projecao': (realizado_emp/13*21), 'atingimento_proj': ating_v_geral, 'cor': "#4caf50"},
                         clie_proj={'meta': t_meta_c, 'realizado': t_venda_c, 'valor_projecao': v_proj_clie, 'atingimento_proj': 0, 'cor': "#4caf50"},
                         geral_clie={'limite': t_lim, 'debito': t_deb, 'atraso': q_atr})

@app.route('/analise/<int:cliente_id>')
@login_required
def analise_cliente(cliente_id):
    """Análise do Cliente tratando valores nulos no limite e débito."""
    res = execute_query(f"SELECT Codigo, Razao_Social, Limite_Credito, Total_Debito FROM clien WHERE Codigo = {cliente_id}")
    if not res: return redirect(url_for('dashboard'))
    
    lim_cli = float(res[0][2] or 0)
    deb_cli = float(res[0][3] or 0)
    
    titulos_raw = execute_query(f"SELECT Num_Documento, Par_Documento, Vlr_Documento, Vlr_Saldo, Dat_Emissao, Dat_Vencimento FROM CTREC WHERE Cod_Cliente = {cliente_id} AND Vlr_Saldo > 0 AND Status IN ('A', 'P') ORDER BY Dat_Vencimento ASC")
    
    hoje = datetime.now().date()
    titulos_processados, dias_atraso_max = [], 0

    for t in titulos_raw:
        vencimento = t[5]
        if isinstance(vencimento, datetime): vencimento = vencimento.date()
        atraso = (hoje - vencimento).days if vencimento and vencimento < hoje else 0
        if atraso > dias_atraso_max: dias_atraso_max = atraso
        titulos_processados.append(list(t) + [atraso])

    v_atual = float(execute_query(f"SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WHERE Cod_Cliente = {cliente_id} AND Status = 'F' AND MONTH(Dat_Emissao) = 1")[0][0] or 0)
    raw_hist = execute_query(f"SELECT MONTH(Dat_Emissao), YEAR(Dat_Emissao), SUM(Vlr_TotalNota) FROM NFSCB WHERE Cod_Cliente = {cliente_id} AND Status = 'F' AND YEAR(Dat_Emissao) IN (2024, 2025, 2026) GROUP BY MONTH(Dat_Emissao), YEAR(Dat_Emissao) ORDER BY 1, 2")
    
    meses = ['JAN','FEV','MAR','ABR','MAI','JUN','JUL','AGO','SET','OUT','NOV','DEZ']
    comp = {i: {'mes': meses[i-1], '2024': 0, '2025': 0, '2026': 0} for i in range(1, 13)}
    for r in raw_hist: comp[r[0]][str(r[1])] = float(r[2] or 0)
    
    return render_template('analise_cliente.html', cliente=res[0], comparativo=list(comp.values()), 
                           limite_credito=lim_cli, saldo=(lim_cli - deb_cli), 
                           dias_atraso=dias_atraso_max, objetivo=get_objetivos_excel().get(cliente_id, 0), 
                           vendas_atual=v_atual, titulos=titulos_processados)

@app.route('/mapa')
@login_required
def mapa_vendas():
    """Mantém a lógica regional e operadores."""
    inicio_raw = request.args.get('inicio', '2026-01-01')
    fim_raw = request.args.get('fim', '2026-01-31')
    vendedor_id = request.args.get('vendedor', '')
    vendedores = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado = 0 ORDER BY Nome_guerra")
    regioes, chart_ml, stats = {}, [], {'movel_qtd': 0, 'movel_vlr': 0.0, 'eletro_qtd': 0, 'eletro_vlr': 0.0, 'total_qtd': 0, 'total_vlr': 0.0, 'clientes_atendidos': 0, 'operadores': {}}

    if vendedor_id:
        d_ini, d_fim = inicio_raw.replace("-", ""), fim_raw.replace("-", "")
        res_cli = execute_query(f"SELECT COUNT(DISTINCT Cod_Cliente) FROM nfscb WITH (NOLOCK) WHERE Status = 'F' AND Cod_Vendedor = {vendedor_id} AND Dat_Emissao BETWEEN '{d_ini}' AND '{d_fim} 23:59:59'")
        stats['clientes_atendidos'] = int(res_cli[0][0]) if res_cli else 0
        
        query = f"""SELECT Cidade, Bairro, Cod_OrigemNfs, SUM(Vlr_TotalNota), COUNT(Num_Nota), 
        ISNULL((SELECT Nome_Guerra FROM VENDE WHERE Codigo = nf.Cod_VendTlmkt), 'NAO ID.') 
        FROM nfscb nf WITH (NOLOCK) WHERE Cod_Estabe = 0 AND Status = 'F' AND Cod_Vendedor = {vendedor_id} 
        AND Dat_Emissao BETWEEN '{d_ini}' AND '{d_fim} 23:59:59' GROUP BY Cidade, Bairro, Cod_OrigemNfs, Cod_VendTlmkt"""
        res = execute_query(query)
        for r in res:
            cid, bai, ori, vlr, qtd, ope = r[0].strip(), r[1].strip(), r[2], float(r[3]), int(r[4]), r[5]
            if ori == 'ML': stats['movel_qtd'] += qtd; stats['movel_vlr'] += vlr; chart_ml.append({'label': f"{cid} - {bai}", 'valor': vlr})
            elif ori == 'TL': stats['eletro_qtd'] += qtd; stats['eletro_vlr'] += vlr
            stats['total_qtd'] += qtd; stats['total_vlr'] += vlr
            stats['operadores'][ope] = stats['operadores'].get(ope, 0) + qtd
            if cid not in regioes: regioes[cid] = {}
            if bai not in regioes[cid]: regioes[cid][bai] = {'ML': [0,0], 'total': 0.0}
            if ori == 'ML': regioes[cid][bai]['ML'][0] += vlr; regioes[cid][bai]['ML'][1] += qtd
            regioes[cid][bai]['total'] += vlr
        chart_ml = sorted(chart_ml, key=lambda x: x['valor'], reverse=True)[:10]

    return render_template('mapa.html', regioes=regioes, vendedores=vendedores, chart_ml=chart_ml, data_inicio=inicio_raw, data_fim=fim_raw, vendedor_selecionado=vendedor_id, stats=stats)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)