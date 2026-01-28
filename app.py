from flask import Flask, render_template, request, session, redirect, url_for
from flask_session import Session
import pyodbc
import json
import os
import logging
import pandas as pd
from datetime import datetime
from functools import wraps

# Configuração de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'varejao_bi_farma_2026_final_regional'
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)

# Caminho da Base de Metas
EXCEL_PATH = r'C:\Projeto_Varejao\bi_flask_app\database\Vlr_ObjetivoClie.xlsx'

# ============================================
# CONEXÃO E UTILITÁRIOS SQL
# ============================================

def execute_query(query):
    """Executa consultas no SQL Server usando configuração externa."""
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
    """Lê as metas individuais de faturamento do arquivo Excel."""
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
# ROTAS DO SISTEMA
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
    """Visão geral com indicadores de faturamento e metas do Excel."""
    filtro = request.args.get('tipo', 'todos')
    valor = request.args.get('valor', '').strip()
    vendedores = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado IN (0, '0') ORDER BY Nome_guerra")
    
    # Parâmetros de Calendário para Projeção
    cal = {'uteis': 21, 'trabalhados': 13}

    # 1. Metas Empresa (Tabela VEOBJ)
    res_m = execute_query("SELECT ISNULL(SUM(Vlr_Cota), 0) FROM VEOBJ WHERE Ano_Ref = 2026 AND Mes_Ref = 1")
    meta_emp = float(res_m[0][0]) if res_m else 0.0
    
    sql_r = "SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WITH (NOLOCK) WHERE Status = 'F' AND MONTH(Dat_Emissao) = 1"
    if filtro == 'vendedor' and valor: sql_r += f" AND Cod_Vendedor = {int(valor)}"
    realizado_emp = float(execute_query(sql_r)[0][0] or 0)
    
    proj_v = (realizado_emp / cal['trabalhados']) * cal['uteis'] if cal['trabalhados'] > 0 else 0
    ating_v = (proj_v / meta_emp * 100) if meta_emp > 0 else 0

    # 2. Dados de Clientes e Metas Excel
    query_clie = """SELECT cl.Codigo, cl.Razao_Social, ISNULL(cl.Limite_Credito, 0), ISNULL(cl.Total_Debito, 0), 
    ISNULL((SELECT MAX(DATEDIFF(DAY, Dat_Vencimento, GETDATE())) FROM CTREC WHERE Cod_Cliente = cl.Codigo AND Vlr_Saldo > 0), 0) as Atr,
    (SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WHERE Cod_Cliente = cl.Codigo AND Status = 'F' AND MONTH(Dat_Emissao) = 1) as Vnd
    FROM clien cl WHERE cl.Bloqueado = 0"""
    res_db = execute_query(query_clie)
    obj_ex = get_objetivos_excel()
    
    clientes_finais, t_meta_c, t_venda_c, t_lim, t_deb, q_atr = [], 0, 0, 0, 0, 0
    for r in res_db:
        meta_c = obj_ex.get(r[0], 0)
        venda = float(r[5] or 0)
        t_meta_c += meta_c; t_venda_c += venda; t_lim += float(r[2]); t_deb += float(r[3])
        if int(r[4]) > 0: q_atr += 1
        ating_c = (venda / meta_c * 100) if meta_c > 0 else 0
        clientes_finais.append([r[0], r[1], 'Não', float(r[2]), float(r[3]), 0, int(r[4]), '', '', 0, venda, meta_c, ating_c])

    return render_template('dashboard.html', clientes=clientes_finais, vendedores=vendedores, filtro_ativo=filtro, valor_filtro=valor,
                         proj={'meta': meta_emp, 'realizado': realizado_emp, 'valor_projecao': proj_v, 'atingimento_proj': ating_v, 'cor': "#4caf50", 'titulo': "Vendas"},
                         clie_proj={'meta': t_meta_c, 'realizado': t_venda_c, 'valor_projecao': (t_venda_c/cal['trabalhados']*cal['uteis']), 'atingimento_proj': 0, 'cor': "#4caf50"},
                         geral_clie={'limite': t_lim, 'debito': t_deb, 'atraso': q_atr})

@app.route('/mapa')
@login_required
def mapa_vendas():
    """Análise Regional Otimizada: Cidade, Bairro e Operadores."""
    inicio_raw = request.args.get('inicio', '2026-01-01')
    fim_raw = request.args.get('fim', '2026-01-31')
    vendedor_id = request.args.get('vendedor', '')
    
    v_list = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado IN (0, '0') ORDER BY Nome_guerra")
    regioes, chart_ml = {}, []
    stats = {'movel_qtd': 0, 'movel_vlr': 0.0, 'eletro_qtd': 0, 'eletro_vlr': 0.0, 'web_qtd': 0, 'web_vlr': 0.0, 'total_qtd': 0, 'total_vlr': 0.0, 'clientes_atendidos': 0, 'operadores': {}}

    if vendedor_id:
        d_ini, d_fim = inicio_raw.replace("-", ""), fim_raw.replace("-", "")
        
        # 1. Contagem de Clientes Únicos (Atendidos)
        sql_cli = f"SELECT COUNT(DISTINCT Cod_Cliente) FROM nfscb WITH (NOLOCK) WHERE Status = 'F' AND Cod_Vendedor = {vendedor_id} AND Dat_Emissao BETWEEN '{d_ini}' AND '{d_fim} 23:59:59'"
        res_cli = execute_query(sql_cli)
        stats['clientes_atendidos'] = int(res_cli[0][0]) if res_cli else 0

        # 2. Dados por Região e Operador
        query = f"""
        SELECT 
            ISNULL(nf.Cidade, 'NAO INF.') as Cid, ISNULL(nf.Bairro, 'NAO INF.') as Bai, nf.Cod_OrigemNfs as Ori, 
            SUM(nf.Vlr_TotalNota) as Vlr, COUNT(nf.Num_Nota) as Qtd, 
            ISNULL(ve_tlm.Nome_Guerra, 'NAO IDENT.') as Operador
        FROM nfscb nf WITH (NOLOCK) 
        LEFT JOIN VENDE ve_tlm ON ve_tlm.Codigo = nf.Cod_VendTlmkt AND ve_tlm.Bloqueado = 0 AND ve_tlm.Cod_TipVenBas = 'TLM'
        WHERE nf.Cod_Estabe = 0 AND nf.Tip_Saida = 'V' AND nf.Status = 'F' 
          AND nf.Cod_Vendedor = {vendedor_id} 
          AND nf.Dat_Emissao >= CAST('{d_ini}' AS DATETIME) AND nf.Dat_Emissao <= CAST('{d_fim} 23:59:59' AS DATETIME)
        GROUP BY nf.Cidade, nf.Bairro, nf.Cod_OrigemNfs, ve_tlm.Nome_Guerra
        ORDER BY nf.Cidade, nf.Bairro
        """
        res = execute_query(query)

        for r in res:
            cid, bai, ori, vlr, qtd, ope = r[0].strip(), r[1].strip(), r[2], float(r[3]), int(r[4]), r[5]
            
            if ori == 'ML': 
                stats['movel_qtd'] += qtd; stats['movel_vlr'] += vlr
                chart_ml.append({'label': f"{cid} - {bai}", 'valor': vlr})
            elif ori == 'TL': stats['eletro_qtd'] += qtd; stats['eletro_vlr'] += vlr
            elif ori == 'WL': stats['web_qtd'] += qtd; stats['web_vlr'] += vlr
            
            stats['total_qtd'] += qtd; stats['total_vlr'] += vlr
            stats['operadores'][ope] = stats['operadores'].get(ope, 0) + qtd

            # Agrupamento para Tabela Regional
            if cid not in regioes: regioes[cid] = {}
            if bai not in regioes[cid]: regioes[cid][bai] = {'ML': [0,0], 'TL': [0,0], 'WL': [0,0], 'total': 0.0}
            if ori in ['ML', 'TL', 'WL']:
                regioes[cid][bai][ori][0] += vlr; regioes[cid][bai][ori][1] += qtd
            regioes[cid][bai]['total'] += vlr

        # Preparação do Top 10 Bairros (Móvel) para o Gráfico
        chart_ml = sorted(chart_ml, key=lambda x: x['valor'], reverse=True)[:10]

    return render_template('mapa.html', regioes=regioes, vendedores=v_list, chart_ml=chart_ml,
                           data_inicio=inicio_raw, data_fim=fim_raw, vendedor_selecionado=vendedor_id, stats=stats)

@app.route('/analise/<int:cliente_id>')
@login_required
def analise_cliente(cliente_id):
    """Histórico Detalhado do Cliente."""
    res = execute_query(f"SELECT Codigo, Razao_Social, Limite_Credito, Total_Debito FROM clien WHERE Codigo = {cliente_id}")
    if not res: return redirect(url_for('dashboard'))
    
    titulos = execute_query(f"SELECT Num_Documento, Par_Documento, Vlr_Documento, Vlr_Saldo, Dat_Emissao, Dat_Vencimento, DATEDIFF(DAY, Dat_Vencimento, GETDATE()) FROM CTREC WHERE Cod_Cliente = {cliente_id} AND Vlr_Saldo > 0")
    dias_atraso_max = max([int(t[6]) for t in titulos if int(t[6]) > 0] or [0])
    
    v_atual = float(execute_query(f"SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WHERE Cod_Cliente = {cliente_id} AND Status = 'F' AND MONTH(Dat_Emissao) = 1")[0][0] or 0)
    
    raw_hist = execute_query(f"SELECT MONTH(Dat_Emissao), YEAR(Dat_Emissao), SUM(Vlr_TotalNota) FROM NFSCB WHERE Cod_Cliente = {cliente_id} AND Status = 'F' AND YEAR(Dat_Emissao) IN (2024, 2025, 2026) GROUP BY MONTH(Dat_Emissao), YEAR(Dat_Emissao) ORDER BY 1, 2")
    meses = ['JAN','FEV','MAR','ABR','MAI','JUN','JUL','AGO','SET','OUT','NOV','DEZ']
    comp = {i: {'mes': meses[i-1], '2024': 0, '2025': 0, '2026': 0} for i in range(1, 13)}
    for r in raw_hist: comp[r[0]][str(r[1])] = float(r[2] or 0)
    
    return render_template('analise_cliente.html', cliente=res[0], comparativo=list(comp.values()), limite_credito=float(res[0][2]), saldo=float(res[0][2]-res[0][3]), dias_atraso=dias_atraso_max, objetivo=get_objetivos_excel().get(cliente_id, 0), vendas_atual=v_atual, titulos=titulos)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)