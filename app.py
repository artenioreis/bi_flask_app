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
app.config['SECRET_KEY'] = 'varejao_bi_farma_2026_v44_7_blindada'
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)

CONFIG_PATH = os.path.join(app.root_path, 'database', 'config.json')
USERS_PATH = os.path.join(app.root_path, 'database', 'users.json')
EXCEL_PATH = r'C:\Projeto_Varejao\bi_flask_app\database\Vlr_ObjetivoClie.xlsx'

# ============================================
# GESTÃO DE USUÁRIOS
# ============================================

def load_users():
    if not os.path.exists(USERS_PATH):
        os.makedirs(os.path.dirname(USERS_PATH), exist_ok=True)
        initial = {"admin": {"nome": "Administrador", "senha": "admin123456"}}
        with open(USERS_PATH, 'w', encoding='utf-8') as f:
            json.dump(initial, f, indent=4)
        return initial
    with open(USERS_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_users(users):
    with open(USERS_PATH, 'w', encoding='utf-8') as f:
        json.dump(users, f, indent=4)

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
            df.columns = df.columns.str.strip()
            df['Codigo'] = pd.to_numeric(df['Codigo'], errors='coerce').fillna(0).astype(int)
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
# ROTAS DE ACESSO E GESTÃO
# ============================================

@app.route('/')
def index():
    return redirect(url_for('dashboard')) if 'user' in session else redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        user_in = request.form.get('username', '').strip()
        pwd_in = request.form.get('password', '').strip()
        users = load_users()
        if user_in in users and users[user_in]['senha'] == pwd_in:
            session['user'] = user_in
            return redirect(url_for('dashboard'))
        return render_template('login.html', erro="Acesso Negado!", config=get_db_cfg())
    return render_template('login.html', config=get_db_cfg())

@app.route('/configurar_banco', methods=['POST'])
def configurar_banco():
    try:
        config = {
            "server": request.form.get('server'),
            "database": request.form.get('database'),
            "username": request.form.get('username'),
            "password": request.form.get('password')
        }
        os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
        with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4)
        flash("Configurações do banco atualizadas com sucesso!", "success")
    except Exception as e:
        flash(f"Erro ao salvar configurações: {e}", "danger")
    return redirect(url_for('login'))

def get_db_cfg():
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f: return json.load(f)
        except: pass
    return {}

@app.route('/usuarios', methods=['GET', 'POST'])
@login_required
def gerenciar_usuarios():
    users = load_users()
    if request.method == 'POST':
        nome = request.form.get('nome'); login_id = request.form.get('login').strip(); senha = request.form.get('senha')
        if login_id and login_id not in users:
            users[login_id] = {"nome": nome, "senha": senha}
            save_users(users)
        return redirect(url_for('gerenciar_usuarios'))
    user_list = [[k, v['nome'], k] for k, v in users.items()]
    return render_template('usuarios.html', usuarios=user_list)

@app.route('/usuarios/excluir/<login_id>')
@login_required
def excluir_usuario(login_id):
    users = load_users()
    if login_id != 'admin' and login_id in users:
        del users[login_id]; save_users(users)
    return redirect(url_for('gerenciar_usuarios'))

@app.route('/usuarios/editar', methods=['POST'])
@login_required
def editar_usuario():
    users = load_users(); login_id = request.form.get('edit_login')
    if login_id in users:
        users[login_id]['nome'] = request.form.get('edit_nome')
        ns = request.form.get('edit_senha')
        if ns: users[login_id]['senha'] = ns
        save_users(users)
    return redirect(url_for('gerenciar_usuarios'))

@app.route('/logout')
def logout():
    session.clear(); return redirect(url_for('login'))

# ============================================
# DASHBOARD v45.1 (AJUSTADO COM SQL LÍQUIDO)
# ============================================

@app.route('/dashboard')
@login_required
def dashboard():
    filtro = request.args.get('tipo', 'todos')
    valor = request.args.get('valor', '').strip()
    
    hoje = date.today()
    hoje_str = hoje.strftime('%Y%m%d')
    primeiro_dia = hoje.replace(day=1).strftime('%Y%m%d')
    mes, ano = hoje.month, hoje.year
    cal = {'uteis': 21, 'trabalhados': 13}
    obj_ex = get_objetivos_excel()
    v_list = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado = 0 ORDER BY Nome_guerra")

    # SQL Ajustado conforme solicitação para Vendedor/Cia (Vlr_Liq)
    def get_sql_vendas_ajustado(cod_vendedor=None):
        filtro_vendedor = f"AND ve.Codigo = {int(cod_vendedor)}" if cod_vendedor else ""
        filtro_vendedor_nfe = f"AND vd.Codigo = {int(cod_vendedor)}" if cod_vendedor else ""
        
        return f"""
        SELECT SUM(y.VlrLiq) FROM (
            SELECT VlrLiq = Sum(it.Vlr_LiqItem-it.Vlr_SubsTrib-it.Vlr_SbtRes-it.Vlr_RecSbt-it.Vlr_SubsTribEmb-it.Vlr_DespRateada-IsNull(it.Vlr_DspExt, 0))
            FROM NFSCB cb
            INNER JOIN NFSIT it ON (cb.Cod_Estabe = it.Cod_Estabe AND cb.Ser_Nota = it.Ser_Nota AND cb.Num_Nota = it.Num_Nota)
            INNER JOIN CLIEN cl ON(cb.Cod_Cliente=cl.Codigo)
            INNER JOIN ENXES en ON((en.Cod_Estabe=cb.Cod_Estabe)AND(cl.Cgc_Cpf=en.Num_CgcCpf)AND(cl.Codigo=en.Cod_Client))
            INNER JOIN VENDE ve ON cb.Cod_Vendedor = ve.Codigo
            INNER JOIN SUPER su ON ve.Cod_Supervisor = su.Codigo
            INNER JOIN (
                SELECT n.Cod_Cliente, v.Codigo
                FROM NFSCB n
                INNER JOIN CLIEN c ON(n.Cod_Cliente=c.Codigo)
                INNER JOIN ENXES e ON((e.Cod_Estabe=n.Cod_Estabe)AND(c.Cgc_Cpf=e.Num_CgcCpf)AND(c.Codigo=e.Cod_Client))
                LEFT OUTER JOIN VENDE v ON (n.Cod_Vendedor = v.Codigo)
                WHERE n.Dat_Emissao >= '{primeiro_dia}' AND n.Dat_Emissao <= '{hoje_str}'
                AND n.Cod_Estabe = 0 AND (n.Status = 'F' AND n.Tip_Saida = 'V')
                GROUP BY n.Cod_Cliente, v.Codigo
            ) x ON(cb.Cod_Cliente=x.Cod_Cliente AND ve.Codigo=x.Codigo)
            WHERE cb.Cod_Estabe = 0 AND (cb.Status = 'F' AND cb.Tip_Saida = 'V')
            AND cb.Dat_Emissao >= '{primeiro_dia}' AND cb.Dat_Emissao <= '{hoje_str}'
            AND su.Cod_Gerencia = 2 {filtro_vendedor}
            
            UNION ALL 
            
            SELECT VlrLiq = (Sum(it.Vlr_LiqIte-it.Vlr_SubsTrib-it.Vlr_DifTri-it.Vlr_DespRateada-it.Vlr_SbtRes) * (-1))
            FROM NFECB cb
            INNER JOIN NFEIT it ON((cb.Cod_Estabe=it.Cod_Estabe)AND(cb.Protocolo=it.Protocolo))
            INNER JOIN VENDE vd ON cb.Cod_Vendedor=vd.Codigo
            INNER JOIN SUPER su ON vd.Cod_Supervisor=su.Codigo
            WHERE cb.Cod_Estabe = 0 AND (cb.Status = 'F' AND cb.Tip_NF = 'D')
            AND cb.Dat_Movimento >= '{primeiro_dia}' AND cb.Dat_Movimento <= '{hoje_str}'
            AND su.Cod_Gerencia = 2 {filtro_vendedor_nfe}
        ) Y
        """

    # Realizado Cia (Líquido)
    res_cia_total = execute_query(get_sql_vendas_ajustado())
    r_cia = float(res_cia_total[0][0] or 0) if res_cia_total else 0.0
    
    res_cia_meta = execute_query(f"SELECT ISNULL(SUM(o.Vlr_Cota), 0) FROM VEOBJ o INNER JOIN vende v ON o.Cod_Vendedor = v.Codigo WHERE o.Ano_Ref = {ano} AND o.Mes_Ref = {mes} AND v.Bloqueado = 0")
    m_cia = float(res_cia_meta[0][0] or 1) if res_cia_meta else 1.0
    
    m_sel, r_sel, p_sel, a_sel = 0, 0, 0, 0
    v_stats = {'total_carteira': 0, 'atendidos': 0}
    
    if filtro == 'vendedor' and valor:
        res_v_meta = execute_query(f"SELECT ISNULL(SUM(Vlr_Cota), 0) FROM VEOBJ WHERE Cod_Vendedor = {int(valor)} AND Ano_Ref = {ano} AND Mes_Ref = {mes}")
        m_sel = float(res_v_meta[0][0] or 1) if res_v_meta else 1.0
        
        # Realizado Vendedor (Líquido)
        res_v_real = execute_query(get_sql_vendas_ajustado(valor))
        r_sel = float(res_v_real[0][0] or 0) if res_v_real else 0.0
        
        p_sel = (r_sel / cal['trabalhados'] * cal['uteis'])
        a_sel = (r_sel / m_sel * 100) if m_sel > 0 else 0
        
        res_v_cart = execute_query(f"SELECT COUNT(DISTINCT Cod_Client) FROM enxes WHERE Cod_Vendedor = {int(valor)} AND Cod_Estabe = 0")
        v_stats['total_carteira'] = int(res_v_cart[0][0] or 0) if res_v_cart else 0
        
        res_v_atend = execute_query(f"SELECT COUNT(DISTINCT Cod_Cliente) FROM NFSCB WHERE Cod_Vendedor = {int(valor)} AND Status = 'F' AND Cod_Estabe = 0 AND MONTH(Dat_Emissao) = {mes} AND YEAR(Dat_Emissao) = {ano}")
        v_stats['atendidos'] = int(res_v_atend[0][0] or 0) if res_v_atend else 0

    clientes_finais, t_m_c, t_v_c = [], 0, 0
    query_clie = f"""SELECT cl.Codigo, cl.Razao_Social, ISNULL(cl.Limite_Credito, 0), ISNULL(cl.Total_Debito, 0), 
    (SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WHERE Cod_Cliente = cl.Codigo AND Status = 'F' AND Cod_Estabe = 0 AND MONTH(Dat_Emissao) = {mes} AND YEAR(Dat_Emissao) = {ano}) as Vnd
    FROM clien cl INNER JOIN enxes en ON cl.Codigo = en.Cod_Client AND en.Cod_Estabe = 0 WHERE cl.Bloqueado = 0"""
    
    if filtro == 'vendedor' and valor: query_clie += f" AND en.Cod_Vendedor = {int(valor)}"
    elif filtro == 'cliente' and valor: query_clie += f" AND (cl.Codigo LIKE '%{valor}%' OR cl.Razao_Social LIKE '%{valor}%')"

    res_db = execute_query(query_clie)
    for r in res_db:
        m_c = obj_ex.get(r[0], 0); vnd = float(r[4] or 0)
        t_m_c += m_c; t_v_c += vnd
        res_at = execute_query(f"SELECT MIN(Dat_Vencimento) FROM CTREC WHERE Cod_Cliente = {r[0]} AND Vlr_Saldo > 0 AND Status IN ('A', 'P')")
        atr_d = 0
        if res_at and res_at[0][0]:
            venc = res_at[0][0].date() if isinstance(res_at[0][0], datetime) else res_at[0][0]
            if venc < hoje: atr_d = (hoje - venc).days
        if valor or len(res_db) < 150:
            clientes_finais.append([r[0], r[1], 'Não', float(r[2]), float(r[3]), 0, atr_d, '', '', 0, vnd, m_c, (vnd/m_c*100 if m_c>0 else 0)])

    return render_template('dashboard.html', clientes=clientes_finais, vendedores=v_list, filtro_ativo=filtro, valor_filtro=valor,
                         proj={'meta': m_cia, 'realizado': r_cia, 'valor_projecao': (r_cia/cal['trabalhados']*cal['uteis']), 'atingimento_proj': (r_cia/m_cia*100)},
                         sel={'meta': m_sel, 'realizado': r_sel, 'valor_projecao': p_sel, 'atingimento_proj': a_sel},
                         clie_proj={'meta': t_m_c, 'realizado': t_v_c, 'valor_projecao': (t_v_c/cal['trabalhados']*cal['uteis']), 'atingimento_proj': (t_v_c/t_m_c*100 if t_m_c>0 else 0)},
                         vendedor_stats=v_stats)

# ============================================
# ANÁLISE CLIENTE
# ============================================

@app.route('/analise/<int:cliente_id>')
@login_required
def analise_cliente(cliente_id):
    mes, ano = datetime.now().month, datetime.now().year
    hoje = date.today()
    inicio_raw = request.args.get('inicio', hoje.replace(day=1).strftime('%Y-%m-%d'))
    fim_raw = request.args.get('fim', hoje.strftime('%Y-%m-%d'))
    d_ini, d_fim = inicio_raw.replace("-", ""), fim_raw.replace("-", "")

    res = execute_query(f"SELECT Codigo, Razao_Social, ISNULL(Limite_Credito, 0), ISNULL(Total_Debito, 0) FROM clien WHERE Codigo = {cliente_id}")
    if not res: return redirect(url_for('dashboard'))
    
    titulos = execute_query(f"SELECT Num_Documento, Par_Documento, Vlr_Documento, Vlr_Saldo, Dat_Emissao, Dat_Vencimento, DATEDIFF(DAY, Dat_Vencimento, GETDATE()) FROM CTREC WHERE Cod_Cliente = {cliente_id} AND Vlr_Saldo > 0")
    d_atr_max = max([int(t[6]) for t in titulos if int(t[6]) > 0] or [0])
    
    res_v_at = execute_query(f"SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB WHERE Cod_Cliente = {cliente_id} AND Status = 'F' AND Cod_Estabe = 0 AND MONTH(Dat_Emissao) = {mes} AND YEAR(Dat_Emissao) = {ano}")
    v_at = float(res_v_at[0][0] or 0) if res_v_at else 0.0
    
    sql_hist = f"SELECT YEAR(Dat_Emissao), MONTH(Dat_Emissao), SUM(Vlr_TotalNota) FROM NFSCB WITH (NOLOCK) WHERE Cod_Cliente = {cliente_id} AND Status = 'F' AND Cod_Estabe = 0 AND YEAR(Dat_Emissao) IN (2024, 2025, 2026) GROUP BY YEAR(Dat_Emissao), MONTH(Dat_Emissao) ORDER BY 1, 2"
    res_hist = execute_query(sql_hist)
    comparativo_data = [{'ano': int(h[0]), 'mes': int(h[1]), 'total': float(h[2])} for h in res_hist]
    
    sql_lab = f"""SELECT 
        ISNULL(fb.Fantasia, 'OUTROS'), 
        SUM(i.Vlr_TotItem) 
        FROM NFSCB n WITH (NOLOCK)
        INNER JOIN nfsit i WITH (NOLOCK) ON n.Num_Nota = i.Num_Nota AND n.Ser_Nota = i.Ser_Nota AND n.Cod_Estabe = i.Cod_Estabe
        INNER JOIN PRODU p WITH (NOLOCK) ON i.Cod_Produto = p.Codigo
        LEFT JOIN FABRI fb WITH (NOLOCK) ON p.Cod_Fabricante = fb.Codigo
        WHERE n.Cod_Cliente = {cliente_id} AND n.Status = 'F' AND n.Cod_Estabe = 0
        AND n.Dat_Emissao BETWEEN '{d_ini}' AND '{d_fim} 23:59:59'
        GROUP BY ISNULL(fb.Fantasia, 'OUTROS') ORDER BY 2 DESC"""
    res_lab = execute_query(sql_lab)
    lab_list = [{'nome': str(r[0]).strip(), 'total': float(r[1])} for r in res_lab]

    lab_top1 = lab_list[0]['nome'] if lab_list else "OUTROS"
    
    v_list = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado = 0 ORDER BY Nome_guerra")
    
    return render_template('analise_cliente.html', cliente=res[0], limite_credito=float(res[0][2]), saldo=float(res[0][2]-res[0][3]), 
                         dias_atraso=d_atr_max, comparativo=comparativo_data, objetivo=get_objetivos_excel().get(cliente_id, 0), 
                         vendas_atual=v_at, vendedores=v_list, 
                         laboratorios=lab_list, data_inicio=inicio_raw, data_fim=fim_raw, 
                         lab_nome_hist=lab_top1)

@app.route('/api/historico_lab/<int:cliente_id>/<lab_nome>')
@login_required
def api_historico_lab(cliente_id, lab_nome):
    sql = f"""SELECT YEAR(n.Dat_Emissao), MONTH(n.Dat_Emissao), SUM(i.Vlr_TotItem) 
        FROM NFSCB n WITH (NOLOCK)
        INNER JOIN nfsit i WITH (NOLOCK) ON n.Num_Nota = i.Num_Nota AND n.Ser_Nota = i.Ser_Nota AND n.Cod_Estabe = i.Cod_Estabe
        INNER JOIN PRODU p WITH (NOLOCK) ON i.Cod_Produto = p.Codigo
        LEFT JOIN FABRI fb WITH (NOLOCK) ON p.Cod_Fabricante = fb.Codigo
        WHERE n.Cod_Cliente = {cliente_id} AND n.Status = 'F' AND n.Cod_Estabe = 0
        AND YEAR(n.Dat_Emissao) IN (2024, 2025, 2026)
        AND ISNULL(fb.Fantasia, 'OUTROS') = '{lab_nome}'
        GROUP BY YEAR(n.Dat_Emissao), MONTH(n.Dat_Emissao) ORDER BY 1, 2"""
    res = execute_query(sql)
    return jsonify([{'ano': int(h[0]), 'mes': int(h[1]), 'total': float(h[2])} for h in res])

# ============================================
# MAPA REGIONAL
# ============================================

@app.route('/mapa')
@login_required
def mapa_vendas():
    hoje = date.today()
    inicio_raw = request.args.get('inicio', hoje.replace(day=1).strftime('%Y-%m-%d'))
    fim_raw = request.args.get('fim', hoje.strftime('%Y-%m-%d'))
    vendedor_id = request.args.get('vendedor', '')
    
    v_list = execute_query("SELECT Codigo, Nome_guerra FROM vende WHERE Bloqueado = 0 ORDER BY Nome_guerra")
    regioes, chart_ml, stats = {}, [], {'movel_qtd': 0, 'movel_vlr': 0.0, 'eletro_qtd': 0, 'eletro_vlr': 0.0, 'total_qtd': 0, 'total_vlr': 0.0, 'clientes_atendidos': 0, 'operadores': {}}

    if vendedor_id:
        d_ini, d_fim = inicio_raw.replace("-", ""), fim_raw.replace("-", "")
        query = f"""SELECT ISNULL(nf.Cidade, 'NAO INF.'), ISNULL(nf.Bairro, 'NAO INF.'), nf.Cod_OrigemNfs, SUM(nf.Vlr_TotalNota), COUNT(nf.Num_Nota), ISNULL(ve.Nome_Guerra, 'NAO IDENT.') 
        FROM nfscb nf WITH (NOLOCK) LEFT JOIN VENDE ve ON ve.Codigo = nf.Cod_VendTlmkt
        WHERE nf.Cod_Estabe = 0 AND nf.Status = 'F' AND nf.Cod_Vendedor = {int(vendedor_id)} AND nf.Dat_Emissao BETWEEN '{d_ini}' AND '{d_fim} 23:59:59'
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
        res_clie = execute_query(f"SELECT COUNT(DISTINCT Cod_Cliente) FROM nfscb WHERE Status='F' AND Cod_Estabe=0 AND Cod_Vendedor={int(vendedor_id)} AND Dat_Emissao BETWEEN '{d_ini}' AND '{d_fim} 23:59:59'")
        if res_clie: stats['clientes_atendidos'] = int(res_clie[0][0])

    return render_template('mapa.html', regioes=regioes, vendedores=v_list, chart_ml=chart_ml, data_inicio=inicio_raw, data_fim=fim_raw, vendedor_selecionado=vendedor_id, stats=stats)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)