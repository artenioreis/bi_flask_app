from database.db_connection import db

def run_diagnostics():
    """Executa diagnóstico completo do banco de dados"""

    print("\n" + "="*80)
    print("DIAGNÓSTICO DO BANCO DE DADOS")
    print("="*80 + "\n")

    # 1. Verificar se banco está conectado
    print("1. VERIFICANDO CONEXÃO...")
    if not db.connection:
        print("❌ Banco NÃO está conectado!")
        print("   Tentando conectar com configuração salva...")
        success, msg = db.connect()
        if success:
            print("✅ Conexão estabelecida!")
        else:
            print(f"❌ Erro: {msg}")
            return False
    else:
        print("✅ Banco já está conectado!\n")

    # 2. Listar todas as tabelas
    print("2. TABELAS DISPONÍVEIS:")
    query_tables = """
    SELECT TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
    """
    success, tables = db.execute_query(query_tables)
    if success and tables:
        for table in tables:
            print(f"   ✓ {table['TABLE_NAME']}")
    else:
        print("   ❌ Erro ao listar tabelas")
    print()

    # 3. Verificar tabela VENDE
    print("3. VERIFICANDO TABELA 'vende':")
    query_vende = "SELECT COUNT(*) as total FROM vende"
    success, result = db.execute_query(query_vende)
    if success and result:
        total = result[0]['total'] if result else 0
        print(f"   Total de vendedores: {total}")
        if total > 0:
            query_sample = "SELECT TOP 5 Codigo, Nome_guerra, bloqueado FROM vende"
            success, sample = db.execute_query(query_sample)
            if success and sample:
                print("   Amostra de dados:")
                for row in sample:
                    print(f"      • Código: {row['Codigo']}, Nome: {row['Nome_guerra']}, Bloqueado: {row['bloqueado']}")
    else:
        print("   ❌ Erro ao consultar tabela vende")
    print()

    # 4. Verificar tabela ENXES
    print("4. VERIFICANDO TABELA 'enxes':")
    query_enxes = "SELECT COUNT(*) as total FROM enxes"
    success, result = db.execute_query(query_enxes)
    if success and result:
        total = result[0]['total'] if result else 0
        print(f"   Total de registros: {total}")
        if total > 0:
            query_sample = "SELECT TOP 5 Cod_Client, Cod_Vendedor, Cod_Estabe FROM enxes"
            success, sample = db.execute_query(query_sample)
            if success and sample:
                print("   Amostra de dados:")
                for row in sample:
                    print(f"      • Cliente: {row['Cod_Client']}, Vendedor: {row['Cod_Vendedor']}, Estab: {row['Cod_Estabe']}")
    else:
        print("   ❌ Erro ao consultar tabela enxes")
    print()

    # 5. Verificar tabela CLIEN
    print("5. VERIFICANDO TABELA 'clien':")
    query_clien = "SELECT COUNT(*) as total FROM clien"
    success, result = db.execute_query(query_clien)
    if success and result:
        total = result[0]['total'] if result else 0
        print(f"   Total de clientes: {total}")
        if total > 0:
            query_sample = "SELECT TOP 5 Codigo, Razao_Social, Bloqueado FROM clien"
            success, sample = db.execute_query(query_sample)
            if success and sample:
                print("   Amostra de dados:")
                for row in sample:
                    print(f"      • Código: {row['Codigo']}, Nome: {row['Razao_Social']}, Bloqueado: {row['Bloqueado']}")
    else:
        print("   ❌ Erro ao consultar tabela clien")
    print()

    # 6. Verificar tabela NFSCB
    print("6. VERIFICANDO TABELA 'NFSCB':")
    query_nfscb = "SELECT COUNT(*) as total FROM NFSCB"
    success, result = db.execute_query(query_nfscb)
    if success and result:
        total = result[0]['total'] if result else 0
        print(f"   Total de notas fiscais: {total}")
        if total > 0:
            query_sample = "SELECT TOP 3 Num_Nota, Ser_Nota, Cod_Cliente, Dat_Emissao FROM NFSCB"
            success, sample = db.execute_query(query_sample)
            if success and sample:
                print("   Amostra de dados:")
                for row in sample:
                    print(f"      • NF: {row['Num_Nota']}/{row['Ser_Nota']}, Cliente: {row['Cod_Cliente']}, Data: {row['Dat_Emissao']}")
    else:
        print("   ❌ Erro ao consultar tabela NFSCB")
    print()

    # 7. Testar JOIN entre VENDE e ENXES
    print("7. TESTANDO JOIN VENDE + ENXES:")
    query_join = """
    SELECT COUNT(*) as total
    FROM vende ve
    INNER JOIN enxes en ON ve.Codigo = en.Cod_Vendedor
    WHERE ve.bloqueado = 0 AND en.Cod_Estabe = 0
    """
    success, result = db.execute_query(query_join)
    if success and result:
        total = result[0]['total'] if result else 0
        print(f"   Vendedores com clientes (Estab=0): {total}")
    else:
        print("   ❌ Erro no JOIN")
    print()

    # 8. Testar JOIN entre CLIEN e ENXES
    print("8. TESTANDO JOIN CLIEN + ENXES + VENDE:")
    query_join2 = """
    SELECT COUNT(*) as total
    FROM clien cl
    LEFT JOIN enxes en ON cl.Codigo = en.Cod_Client
    LEFT JOIN vende ve ON en.Cod_Vendedor = ve.Codigo
    WHERE ve.bloqueado = 0
    """
    success, result = db.execute_query(query_join2)
    if success and result:
        total = result[0]['total'] if result else 0
        print(f"   Clientes com vendedor ativo: {total}")
    else:
        print("   ❌ Erro no JOIN")
    print()

    # 9. Verificar estrutura da tabela CLIEN
    print("9. ESTRUTURA DA TABELA 'clien':")
    query_columns = """
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'clien'
    ORDER BY ORDINAL_POSITION
    """
    success, columns = db.execute_query(query_columns)
    if success and columns:
        print("   Colunas disponíveis:")
        for col in columns[:15]:  # Mostrar apenas as 15 primeiras
            print(f"      • {col['COLUMN_NAME']}: {col['DATA_TYPE']}")
        if len(columns) > 15:
            print(f"      ... e mais {len(columns) - 15} colunas")
    print()

    # 10. Verificar estrutura da tabela ENXES
    print("10. ESTRUTURA DA TABELA 'enxes':")
    query_columns = """
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'enxes'
    ORDER BY ORDINAL_POSITION
    """
    success, columns = db.execute_query(query_columns)
    if success and columns:
        print("   Colunas disponíveis:")
        for col in columns:
            print(f"      • {col['COLUMN_NAME']}: {col['DATA_TYPE']}")
    print()

    print("="*80)
    print("FIM DO DIAGNÓSTICO")
    print("="*80 + "\n")

if __name__ == '__main__':
    run_diagnostics()
