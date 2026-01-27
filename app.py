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
app.config['SECRET_KEY'] = 'varejao_bi_farma_2026_final_consolidado'
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)

# Caminhos de Arquivos
EXCEL_PATH = r'C:\Projeto_Varejao\bi_flask_app\database\Vlr_ObjetivoClie.xlsx'
CACHE_FILE = r'C:\Projeto_Varejao\bi_flask_app\database\cache_coords.json'

# ============================================
# GESTÃO DE CACHE (MEMÓRIA LOCAL PARA O MAPA)
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
# ROTAS DO SISTEMA
# ============================================

@app.route('/dashboard')
@login_required
def dashboard():
    filtro = request.args.get('tipo', 'todos')
    valor = request.args.get('valor', '').strip()
    vendedores = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado IN (0, '0') ORDER BY Nome_guerra")
    cal = {'uteis': 21, 'trabalhados': 13, 'restantes': 8}

    # --- 1. INDICADORES EMPRESA (Tabela VEOBJ) ---
    if filtro == 'vendedor' and valor:
        res_m = execute_query(f"SELECT ISNULL(Vlr_Cota, 0) FROM VEOBJ WHERE Cod_Vendedor = {int(valor)} AND Ano_Ref = 2026 AND Mes_Ref = 1")
        meta_emp = float(res_m[0][0]) if res_m else 0.0
        realizado_emp = float(execute_query(f"SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WITH (NOLOCK) WHERE Cod_Vendedor = {int(valor)} AND Status = 'F' AND MONTH(Dat_Emissao) = 1 AND YEAR(Dat_Emissao) = 2026")[0][0] or 0)
        titulo_v = f"Vendas: {next((v[1] for v in vendedores if str(v[0])==valor), 'Vendedor')}"
    else:
        res_m = execute_query("SELECT ISNULL(SUM(Vlr_Cota), 0) FROM VEOBJ WHERE Ano_Ref = 2026 AND Mes_Ref = 1")
        meta_emp = float(res_m[0][0]) if res_m else 0.0
        realizado_emp = float(execute_query("SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WITH (NOLOCK) WHERE Status = 'F' AND MONTH(Dat_Emissao) = 1 AND YEAR(Dat_Emissao) = 2026")[0][0] or 0)
        titulo_v = "Vendas Gerais (Empresa)"

    proj_v = (realizado_emp / cal['trabalhados']) * cal['uteis'] if cal['trabalhados'] > 0 else 0
    ating_v = (proj_v / meta_emp * 100) if meta_emp > 0 else 0

    # --- 2. LISTA DE CLIENTES E CÁLCULO DAS METAS DO EXCEL ---
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
        ating_c = (venda / meta_c * 100) if meta_c > 0 else 0
        clientes_finais.append([r[0], r[1], 'Não', lim, deb, 0, atraso, '', '', 0, venda, meta_c, ating_c])

    v_proj_clie = (total_venda_clie / cal['trabalhados'] * cal['uteis'])
    ating_clie_total = (v_proj_clie / total_meta_clie * 100) if total_meta_clie > 0 else 0

    return render_template('dashboard.html', 
                         clientes=clientes_finais, vendedores=vendedores, filtro_ativo=filtro, valor_filtro=valor,
                         proj={'meta': meta_emp, 'realizado': realizado_emp, 'valor_projecao': proj_v, 'atingimento_proj': ating_v, 'cor': "#4caf50" if ating_v >= 100 else "#ff9800", 'titulo': titulo_v},
                         clie_proj={'meta': total_meta_clie, 'realizado': total_venda_clie, 'valor_projecao': v_proj_clie, 'atingimento_proj': ating_clie_total, 'cor': "#4caf50" if ating_clie_total >= 100 else "#ff9800"},
                         geral_clie={'limite': total_limite, 'debito': total_debito, 'atraso': qtd_atraso})

@app.route('/mapa')
@login_required
def mapa_vendas():
    inicio_raw = request.args.get('inicio', '2026-01-01')
    fim_raw = request.args.get('fim', datetime.now().strftime('%Y-%m-%d'))
    vendedor_id = request.args.get('vendedor', '')

    vendedores = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado IN (0, '0') ORDER BY Nome_guerra")
    pontos = []

    if vendedor_id:
        d_inicio = inicio_raw.replace("-", "")
        d_fim = fim_raw.replace("-", "")
        cache = carregar_cache()
        mudou_cache = False

        query = f"""
        SELECT nf.cep, nf.Endereco, nf.Cidade, nf.Estado, SUM(nf.Vlr_TotalNota), MAX(cl.Razao_Social)
        FROM nfscb nf WITH (NOLOCK)
        JOIN clien cl WITH (NOLOCK) ON nf.Cod_Cliente = cl.Codigo
        WHERE nf.Cod_Estabe = 0 AND nf.Tip_Saida = 'V' AND nf.Status = 'F'
          AND nf.Cod_OrigemNfs IN ('ML', 'TL')
          AND nf.Dat_Emissao >= CAST('{d_inicio}' AS DATETIME) AND nf.Dat_Emissao <= CAST('{d_fim} 23:59:59' AS DATETIME)
          AND (nf.Cod_Vendedor = {vendedor_id} OR nf.Cod_VendTlmkt = {vendedor_id})
        GROUP BY nf.cep, nf.Endereco, nf.Cidade, nf.Estado
        """
        res_mapa = execute_query(query)

        for r in res_mapa:
            chave = str(r[0]).strip().replace("-", "")
            if not chave: continue
            
            if chave in cache:
                lat, lon = cache[chave]['lat'], cache[chave]['lon']
            else:
                try:
                    url = f"https://nominatim.openstreetmap.org/search?format=json&q={chave},Brasil"
                    headers = {'User-Agent': 'VarejaoBI/2.0'}
                    resp = requests.get(url, headers=headers, timeout=5).json()
                    if resp:
                        lat, lon = resp[0]['lat'], resp[0]['lon']
                        cache[chave] = {'lat': lat, 'lon': lon}
                        mudou_cache = True
                        time.sleep(1)
                    else: continue
                except: continue

            pontos.append({'lat': lat, 'lon': lon, 'label': f"<b>{r[5]}</b><br>R$ {float(r[4]):.2f}", 'end': f"{r[1]}, {r[2]}"})

        if mudou_cache: salvar_cache(cache)

    return render_template('mapa.html', pontos=pontos, vendedores=vendedores, data_inicio=inicio_raw, data_fim=fim_raw, vendedor_selecionado=vendedor_id)

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

@app.route('/')
def index():
    return redirect(url_for('dashboard')) if 'user' in session else redirect(url_for('login'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)