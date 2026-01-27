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

# Configuração de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'varejao_bi_farma_2026_fix_v20'
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)

# Caminhos de Arquivos
EXCEL_PATH = r'C:\Projeto_Varejao\bi_flask_app\database\Vlr_ObjetivoClie.xlsx'
CACHE_FILE = r'C:\Projeto_Varejao\bi_flask_app\database\cache_coords.json'

# ============================================
# FUNÇÕES DE APOIO (SQL E CACHE)
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
# ROTAS (Ajustadas para evitar 404)
# ============================================

@app.route('/')
def index():
    """Resolve o erro 404 ao acessar a raiz do site."""
    if 'user' in session:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        session['user'] = request.form.get('username')
        return redirect(url_for('dashboard'))
    return render_template('login.html')

@app.route('/dashboard')
@login_required
def dashboard():
    filtro = request.args.get('tipo', 'todos')
    valor = request.args.get('valor', '').strip()
    vendedores = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado IN (0, '0') ORDER BY Nome_guerra")
    cal = {'uteis': 21, 'trabalhados': 13, 'restantes': 8}

    # Garantindo que as variáveis de metas do Excel sejam calculadas sempre
    res_m = execute_query("SELECT ISNULL(SUM(Vlr_Cota), 0) FROM VEOBJ WHERE Ano_Ref = 2026 AND Mes_Ref = 1")
    meta_emp = float(res_m[0][0]) if res_m else 0.0
    
    # Query de realizado baseada no filtro
    if filtro == 'vendedor' and valor:
        sql_realizado = f"SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WITH (NOLOCK) WHERE Cod_Vendedor = {int(valor)} AND Status = 'F' AND MONTH(Dat_Emissao) = 1 AND YEAR(Dat_Emissao) = 2026"
    else:
        sql_realizado = "SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WITH (NOLOCK) WHERE Status = 'F' AND MONTH(Dat_Emissao) = 1 AND YEAR(Dat_Emissao) = 2026"
    
    realizado_emp = float(execute_query(sql_realizado)[0][0] or 0)
    proj_v = (realizado_emp / cal['trabalhados']) * cal['uteis'] if cal['trabalhados'] > 0 else 0
    ating_v = (proj_v / meta_emp * 100) if meta_emp > 0 else 0

    # Lógica de Clientes para a Tabela e Metas Excel
    query_clie = """
    SELECT cl.Codigo, cl.Razao_Social, ISNULL(cl.Limite_Credito, 0), ISNULL(cl.Total_Debito, 0), 
    ISNULL((SELECT MAX(DATEDIFF(DAY, DATEADD(DAY, ISNULL(Qtd_DiaExtVct, 0), Dat_Vencimento), GETDATE()))
            FROM CTREC WITH (NOLOCK) WHERE Cod_Cliente = cl.Codigo AND Vlr_Saldo > 0 AND Status IN ('A', 'P') AND Dat_Vencimento < GETDATE()), 0) AS Atraso,
    (SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WITH (NOLOCK) WHERE Cod_Cliente = cl.Codigo AND Status = 'F' AND MONTH(Dat_Emissao) = 1 AND YEAR(Dat_Emissao) = 2026)
    FROM clien cl WITH (NOLOCK)
    LEFT JOIN enxes en ON cl.Codigo = en.Cod_Client AND en.Cod_Estabe = 0
    WHERE cl.Bloqueado IN (0, '0') """

    if filtro == 'vendedor' and valor: query_clie += f" AND en.Cod_Vendedor = {int(valor)}"
    res_db = execute_query(query_clie)
    obj_excel = get_objetivos_excel()
    
    clientes_finais, t_meta_clie, t_venda_clie = [], 0, 0
    t_lim, t_deb, q_atraso = 0, 0, 0

    for r in res_db:
        lim, deb, atraso, venda = float(r[2] or 0), float(r[3] or 0), int(r[4] or 0), float(r[5] or 0)
        meta_c = obj_excel.get(r[0], 0)
        t_meta_clie += meta_c; t_venda_clie += venda; t_lim += lim; t_deb += deb
        if atraso > 0: q_atraso += 1
        ating_c = (venda / meta_c * 100) if meta_c > 0 else 0
        clientes_finais.append([r[0], r[1], 'Não', lim, deb, 0, atraso, '', '', 0, venda, meta_c, ating_c])

    v_proj_clie = (t_venda_clie / cal['trabalhados'] * cal['uteis'])
    ating_clie_total = (v_proj_clie / t_meta_clie * 100) if t_meta_clie > 0 else 0

    return render_template('dashboard.html', 
                         clientes=clientes_finais, vendedores=vendedores, filtro_ativo=filtro, valor_filtro=valor,
                         proj={'meta': meta_emp, 'realizado': realizado_emp, 'valor_projecao': proj_v, 'atingimento_proj': ating_v, 'cor': "#4caf50", 'titulo': "Vendas"},
                         clie_proj={'meta': t_meta_clie, 'realizado': t_venda_clie, 'valor_projecao': v_proj_clie, 'atingimento_proj': ating_clie_total, 'cor': "#4caf50"},
                         geral_clie={'limite': t_lim, 'debito': t_deb, 'atraso': q_atraso})

@app.route('/mapa')
@login_required
def mapa_vendas():
    """Garante que as vendas Móvel (ML) apareçam via busca robusta."""
    inicio_raw = request.args.get('inicio', '2026-01-01')
    fim_raw = request.args.get('fim', '2026-01-31')
    vendedor_id = request.args.get('vendedor', '')
    
    vendedores = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado IN (0, '0') ORDER BY Nome_guerra")
    pontos, stats = [], {'movel_qtd': 0, 'movel_vlr': 0.0, 'eletro_qtd': 0, 'eletro_vlr': 0.0, 'web_qtd': 0, 'web_vlr': 0.0, 'total_qtd': 0, 'total_vlr': 0.0}

    if vendedor_id:
        d_ini, d_fim = inicio_raw.replace("-", ""), fim_raw.replace("-", "")
        cache, mudou = carregar_cache(), False
        
        # Query focada em trazer cada cliente individualmente para não 'sumir' com pontos
        query = f"""
        SELECT nf.cep, nf.Endereco, nf.Cidade, nf.Cod_OrigemNfs, SUM(nf.Vlr_TotalNota), COUNT(nf.Num_Nota), cl.Razao_Social
        FROM nfscb nf WITH (NOLOCK) JOIN clien cl WITH (NOLOCK) ON nf.Cod_Cliente = cl.Codigo 
        WHERE nf.Cod_Estabe = 0 AND nf.Tip_Saida = 'V' AND nf.Status = 'F' 
          AND nf.Cod_OrigemNfs IN ('ML', 'TL', 'WL') 
          AND nf.Dat_Emissao >= CAST('{d_ini}' AS DATETIME) AND nf.Dat_Emissao <= CAST('{d_fim} 23:59:59' AS DATETIME) 
          AND (nf.Cod_Vendedor = {vendedor_id} OR nf.Cod_VendTlmkt = {vendedor_id}) 
        GROUP BY nf.cep, nf.Endereco, nf.Cidade, nf.Cod_OrigemNfs, cl.Razao_Social
        """
        res = execute_query(query)
        
        for r in res:
            origem, valor, qtd = r[3], float(r[4]), int(r[5])
            if 'ML' in origem: stats['movel_qtd'] += qtd; stats['movel_vlr'] += valor
            elif 'TL' in origem: stats['eletro_qtd'] += qtd; stats['eletro_vlr'] += valor
            elif 'WL' in origem: stats['web_qtd'] += qtd; stats['web_vlr'] += valor
            stats['total_qtd'] += qtd; stats['total_vlr'] += valor
            
            chave = f"{str(r[0]).strip()}_{r[6]}"
            if chave in cache: lat, lon = cache[chave]['lat'], cache[chave]['lon']
            else:
                try:
                    # Se falhar o endereço completo, tenta só o CEP
                    busca = f"{r[0]}, {r[1]}, {r[2]}, Brasil"
                    resp = requests.get(f"https://nominatim.openstreetmap.org/search?format=json&q={busca}", headers={'User-Agent': 'VarejaoBI/10.0'}, timeout=5).json()
                    if not resp:
                        resp = requests.get(f"https://nominatim.openstreetmap.org/search?format=json&q={r[0]},Brasil", headers={'User-Agent': 'VarejaoBI/10.0'}, timeout=5).json()
                    
                    if resp:
                        lat, lon = resp[0]['lat'], resp[0]['lon']
                        cache[chave] = {'lat': lat, 'lon': lon}; mudou = True; time.sleep(1)
                    else: continue
                except: continue
                
            pontos.append({'lat': lat, 'lon': lon, 'label': f"<b>{r[6]}</b><br>Notas: {qtd}<br>R$ {valor:.2f}", 'end': f"{r[1]}, {r[2]}"})
        
        if mudou: salvar_cache(cache)
        
    return render_template('mapa.html', pontos=pontos, vendedores=vendedores, data_inicio=inicio_raw, data_fim=fim_raw, vendedor_selecionado=vendedor_id, stats=stats)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)