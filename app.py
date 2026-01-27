from flask import Flask, render_template, request, session, redirect, url_for
from flask_session import Session
import pyodbc
import json
import os
import logging
import pandas as pd
import requests
import time
from datetime import datetime
from functools import wraps

# Configuração de Performance e Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'varejao_bi_farma_2026_speed_v25'
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)

# Caminhos de Arquivos
EXCEL_PATH = r'C:\Projeto_Varejao\bi_flask_app\database\Vlr_ObjetivoClie.xlsx'
CACHE_FILE = r'C:\Projeto_Varejao\bi_flask_app\database\cache_coords.json'

# ============================================
# MOTOR DE VELOCIDADE (CACHE)
# ============================================

def carregar_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: return {}
    return {}

def salvar_cache(cache_data):
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache_data, f, indent=4)

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
    filtro = request.args.get('tipo', 'todos')
    valor = request.args.get('valor', '').strip()
    vendedores = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado IN (0, '0') ORDER BY Nome_guerra")
    cal = {'trabalhados': 13}
    
    res_m = execute_query("SELECT ISNULL(SUM(Vlr_Cota), 0) FROM VEOBJ WHERE Ano_Ref = 2026 AND Mes_Ref = 1")
    meta_emp = float(res_m[0][0]) if res_m else 0.0
    sql_r = f"SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WITH (NOLOCK) WHERE Status = 'F' AND MONTH(Dat_Emissao) = 1"
    if filtro == 'vendedor' and valor: sql_r += f" AND Cod_Vendedor = {int(valor)}"
    realizado_emp = float(execute_query(sql_r)[0][0] or 0)
    
    proj_v = (realizado_emp / cal['trabalhados']) * 21 if cal['trabalhados'] > 0 else 0
    ating_v = (proj_v / meta_emp * 100) if meta_emp > 0 else 0

    # Variáveis fundamentais para evitar UndefinedError
    query_clie = """SELECT cl.Codigo, cl.Razao_Social, ISNULL(cl.Limite_Credito, 0), ISNULL(cl.Total_Debito, 0), 
    ISNULL((SELECT MAX(DATEDIFF(DAY, Dat_Vencimento, GETDATE())) FROM CTREC WHERE Cod_Cliente = cl.Codigo AND Vlr_Saldo > 0), 0) as Atr,
    (SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WHERE Cod_Cliente = cl.Codigo AND Status = 'F' AND MONTH(Dat_Emissao) = 1) as Vnd
    FROM clien cl WHERE cl.Bloqueado = 0"""
    res_db = execute_query(query_clie)
    obj_ex = get_objetivos_excel()
    clientes_finais, t_meta_c, t_venda_c, t_lim, t_deb, q_atr = [], 0, 0, 0, 0, 0

    for r in res_db:
        lim, deb, atr, vnd = float(r[2] or 0), float(r[3] or 0), int(r[4] or 0), float(r[5] or 0)
        meta_c = obj_ex.get(r[0], 0)
        t_meta_c += meta_c; t_venda_c += vnd; t_lim += lim; t_deb += deb
        if atr > 0: q_atr += 1
        clientes_finais.append([r[0], r[1], 'Não', lim, deb, 0, atr, '', '', 0, vnd, meta_c, (vnd/meta_c*100 if meta_c>0 else 0)])

    return render_template('dashboard.html', clientes=clientes_finais, vendedores=vendedores, filtro_ativo=filtro, valor_filtro=valor,
                         proj={'meta': meta_emp, 'realizado': realizado_emp, 'valor_projecao': proj_v, 'atingimento_proj': ating_v, 'cor': "#4caf50", 'titulo': "Vendas"},
                         clie_proj={'meta': t_meta_c, 'realizado': t_venda_c, 'valor_projecao': (t_venda_c/cal['trabalhados']*21), 'atingimento_proj': 0, 'cor': "#4caf50"},
                         geral_clie={'limite': t_lim, 'debito': t_deb, 'atraso': q_atr})

@app.route('/mapa')
@login_required
def mapa_vendas():
    """Lógica otimizada para carregar endereços em massa."""
    inicio_raw = request.args.get('inicio', '2026-01-01')
    fim_raw = request.args.get('fim', '2026-01-31')
    vendedor_id = request.args.get('vendedor', '')
    
    vendedores = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado IN (0, '0') ORDER BY Nome_guerra")
    pontos, stats = [], {'movel_qtd': 0, 'movel_vlr': 0.0, 'eletro_qtd': 0, 'eletro_vlr': 0.0, 'web_qtd': 0, 'web_vlr': 0.0, 'total_qtd': 0, 'total_vlr': 0.0, 'operadores': {}}

    if vendedor_id:
        d_ini, d_fim = inicio_raw.replace("-", ""), fim_raw.replace("-", "")
        cache, mudou = carregar_cache(), False
        
        query = f"""SELECT nf.cep, nf.Endereco, nf.Cidade, nf.Cod_OrigemNfs, SUM(nf.Vlr_TotalNota), COUNT(nf.Num_Nota), cl.Razao_Social, ISNULL(ve_tlm.Nome_Guerra, 'Nao Identificado') 
        FROM nfscb nf WITH (NOLOCK) INNER JOIN clien cl WITH (NOLOCK) ON cl.Codigo = nf.Cod_Cliente 
        LEFT JOIN VENDE ve_tlm ON ve_tlm.Codigo = nf.Cod_VendTlmkt AND ve_tlm.Bloqueado = 0 AND ve_tlm.Cod_TipVenBas = 'TLM' 
        WHERE nf.Cod_Estabe = 0 AND nf.Tip_Saida = 'V' AND nf.Status = 'F' AND nf.Cod_Vendedor = {vendedor_id} 
        AND nf.Cod_OrigemNfs IN ('ML', 'TL', 'WL') AND nf.Dat_Emissao >= CAST('{d_ini}' AS DATETIME) AND nf.Dat_Emissao <= CAST('{d_fim} 23:59:59' AS DATETIME) 
        GROUP BY nf.cep, nf.Endereco, nf.Cidade, nf.Cod_OrigemNfs, cl.Razao_Social, ve_tlm.Nome_Guerra"""
        
        res = execute_query(query)
        
        for r in res:
            origem, valor, qtd, operador = r[3], float(r[4]), int(r[5]), r[7]
            if 'ML' in origem: stats['movel_qtd'] += qtd; stats['movel_vlr'] += valor
            elif 'TL' in origem: stats['eletro_qtd'] += qtd; stats['eletro_vlr'] += valor
            elif 'WL' in origem: stats['web_qtd'] += qtd; stats['web_vlr'] += valor
            stats['total_qtd'] += qtd; stats['total_vlr'] += valor
            stats['operadores'][operador] = stats['operadores'].get(operador, 0) + qtd
            
            # CHAVE ÚNICA PARA VELOCIDADE
            chave = str(r[0]).strip().replace("-", "")
            if not chave: continue
            
            if chave in cache:
                lat, lon = cache[chave]['lat'], cache[chave]['lon']
            else:
                try:
                    # Busca apenas se não houver no cache para não travar a tela
                    resp = requests.get(f"https://nominatim.openstreetmap.org/search?format=json&q={chave},Brasil", headers={'User-Agent': 'VarejaoSpeed/1.0'}, timeout=3).json()
                    if resp: 
                        lat, lon = resp[0]['lat'], resp[0]['lon']
                        cache[chave] = {'lat': lat, 'lon': lon}; mudou = True
                        time.sleep(1) # Delay obrigatório apenas para novos endereços
                    else: continue
                except: continue
                
            pontos.append({'lat': lat, 'lon': lon, 'label': f"<b>{r[6]}</b><br>Notas: {qtd}<br>R$ {valor:.2f}", 'end': f"{r[1]}, {r[2]}"})
        
        if mudou: salvar_cache(cache)
        
    return render_template('mapa.html', pontos=pontos, vendedores=vendedores, data_inicio=inicio_raw, data_fim=fim_raw, vendedor_selecionado=vendedor_id, stats=stats)

@app.route('/analise/<int:cliente_id>')
@login_required
def analise_cliente(cliente_id):
    """Restauração completa da variável dias_atraso."""
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