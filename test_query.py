import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.db_connection import db

print("\n" + "="*80)
print("TESTE DE QUERY - DASHBOARD")
print("="*80 + "\n")

# Teste 1: Query simples
print("1. TESTE SIMPLES - Contar clientes:")
query1 = "SELECT COUNT(*) as total FROM clien WHERE Bloqueado = '0'"
success, result = db.execute_query(query1)
if success and result:
    print(f"   ✅ Total de clientes: {result[0]['total']}\n")
else:
    print(f"   ❌ Erro na query\n")

# Teste 2: Query com TOP
print("2. TESTE COM TOP - Primeiros 10 clientes:")
query2 = """
SELECT TOP 10
    cl.Codigo,
    cl.Razao_Social,
    cl.Bloqueado,
    cl.Limite_Credito,
    cl.Total_Debito
FROM clien cl
WHERE cl.Bloqueado = '0'
ORDER BY cl.Razao_Social
"""
success, result = db.execute_query(query2)
if success and result:
    print(f"   ✅ Clientes encontrados: {len(result)}")
    for row in result[:3]:
        print(f"      • {row['Codigo']} - {row['Razao_Social']}")
    print()
else:
    print(f"   ❌ Erro na query\n")

# Teste 3: Query com LEFT JOIN
print("3. TESTE COM LEFT JOIN - Clientes com vendedor:")
query3 = """
SELECT TOP 10
    cl.Codigo,
    cl.Razao_Social,
    ISNULL(ve.Nome_guerra, 'N/A') AS Vendedor
FROM clien cl
LEFT JOIN enxes en ON cl.Cgc_Cpf = en.Num_CgcCpf AND en.Cod_Estabe = 0
LEFT JOIN vende ve ON en.Cod_Vendedor = ve.Codigo AND ve.Bloqueado = 0
WHERE cl.Bloqueado = '0'
ORDER BY cl.Razao_Social
"""
success, result = db.execute_query(query3)
if success and result:
    print(f"   ✅ Clientes encontrados: {len(result)}")
    for row in result[:3]:
        print(f"      • {row['Codigo']} - {row['Razao_Social']} ({row['Vendedor']})")
    print()
else:
    print(f"   ❌ Erro na query\n")

# Teste 4: Query com subqueries (como no dashboard)
print("4. TESTE COM SUBQUERIES - Query do dashboard:")
query4 = """
DECLARE @DatIni DATETIME = '01-01-2025 00:00:00';
DECLARE @DatFim DATETIME = '31-12-2026 23:59:59';

SELECT TOP 10
    cl.Codigo,
    cl.Razao_Social,
    CASE WHEN cl.Bloqueado = '0' THEN 'Não' ELSE 'Sim' END AS Bloqueado,
    cl.Limite_Credito,
    cl.Total_Debito,
    cl.Data_UltimaFatura,
    ISNULL(ve.Nome_guerra, 'N/A') AS Vendedor,
    (SELECT COUNT(DISTINCT Num_Nota) FROM NFSCB 
     WHERE Cod_Cliente = cl.Codigo 
     AND Status = 'F' 
     AND Dat_Emissao BETWEEN @DatIni AND @DatFim
     AND Cod_Estabe = 0) AS Total_NF,
    (SELECT ISNULL(SUM(Vlr_TotalNota), 0) FROM NFSCB 
     WHERE Cod_Cliente = cl.Codigo 
     AND Status = 'F' 
     AND Dat_Emissao BETWEEN @DatIni AND @DatFim
     AND Cod_Estabe = 0) AS Total_Compras
FROM clien cl
LEFT JOIN enxes en ON cl.Cgc_Cpf = en.Num_CgcCpf AND en.Cod_Estabe = 0
LEFT JOIN vende ve ON en.Cod_Vendedor = ve.Codigo AND ve.Bloqueado = 0
WHERE cl.Bloqueado = '0'
ORDER BY cl.Razao_Social
"""
success, result = db.execute_query(query4)
if success and result:
    print(f"   ✅ Clientes encontrados: {len(result)}")
    for row in result[:3]:
        print(f"      • {row['Codigo']} - {row['Razao_Social']} ({row['Vendedor']})")
        print(f"        Limite: R$ {row['Limite_Credito']}, Débito: R$ {row['Total_Debito']}")
        print(f"        NFs: {row['Total_NF']}, Total Compras: R$ {row['Total_Compras']}")
    print()
else:
    print(f"   ❌ Erro na query\n")

# Teste 5: Vendedores
print("5. TESTE VENDEDORES:")
query5 = """
SELECT DISTINCT ve.Codigo, ve.Nome_guerra
FROM vende ve
WHERE ve.Bloqueado = 0
ORDER BY ve.Nome_guerra
"""
success, result = db.execute_query(query5)
if success and result:
    print(f"   ✅ Vendedores encontrados: {len(result)}")
    for row in result[:5]:
        print(f"      • {row['Codigo']} - {row['Nome_guerra']}")
    print()
else:
    print(f"   ❌ Erro na query\n")

# Teste 6: Grupos
print("6. TESTE GRUPOS:")
query6 = """
SELECT DISTINCT cl.Cod_GrpCli, 
    ISNULL(cl.Descricao, 'Grupo ' + CAST(cl.Cod_GrpCli AS VARCHAR)) AS Descricao
FROM clien cl
WHERE cl.Cod_GrpCli IS NOT NULL
ORDER BY cl.Descricao
"""
success, result = db.execute_query(query6)
if success and result:
    print(f"   ✅ Grupos encontrados: {len(result)}")
    for row in result[:5]:
        print(f"      • {row['Cod_GrpCli']} - {row['Descricao']}")
    print()
else:
    print(f"   ❌ Erro na query\n")

print("="*80)
print("FIM DO TESTE")
print("="*80 + "\n")
