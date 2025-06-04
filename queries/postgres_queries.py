import psycopg2
import time
from datetime import datetime
from decimal import Decimal

def format_currency_br(valor: Decimal) -> str:
    valor_str = f"{valor:,.2f}"  
    return "R$ " + valor_str.replace(",", "X").replace(".", ",").replace("X", ".")

def format_row(row):
    id_pedido, id_cliente, data_pedido, status, valor_total = row
    data_formatada = data_pedido.strftime('%d/%m/%Y %H:%M')
    valor_formatado = format_currency_br(valor_total)
    return f"Pedido #{id_pedido} | Cliente #{id_cliente} | Data: {data_formatada} | Status: {status} | Valor: {valor_formatado}"

def format_produto(row):
    id_produto, nome, categoria, preco, estoque = row
    preco_formatado = format_currency_br(preco)
    return f"Produto #{id_produto} | Nome: {nome} | Categoria: {categoria} | Preço: {preco_formatado} | Estoque: {estoque}"

def format_mais_vendido(row):
    nome, total_vendido = row
    return f"Produto: {nome} | Quantidade vendida: {total_vendido}"

def format_pagamento(row):
    id_pagamento, id_pedido, tipo, status, data_pagamento = row
    data_formatada = data_pagamento.strftime('%d/%m/%Y %H:%M')
    return f"Pagamento #{id_pagamento} | Pedido #{id_pedido} | Tipo: {tipo.upper()} | Status: {status.capitalize()} | Data: {data_formatada}"

def format_total_gasto(row):
    nome, total = row
    total_formatado = format_currency_br(total)
    return f"Cliente: {nome} | Total gasto: {total_formatado}"

def run_query(description, query, params=None, formatter=None):
    conn = psycopg2.connect(
        host="localhost", database="techmarket",
        user="techmarket", password="password"
    )
    cursor = conn.cursor()
    start = time.time()
    cursor.execute(query, params)
    results = cursor.fetchall()
    end = time.time()
    print(f"{description} - Tempo: {end - start:.4f} s")
    if not results:
        print("Nenhum resultado encontrado.")
    for row in results:
        print(formatter(row) if formatter else row)
    cursor.close()
    conn.close()

def get_first_cliente():
    """Busca o primeiro cliente disponível no banco"""
    conn = psycopg2.connect(
        host="localhost", database="techmarket",
        user="techmarket", password="password"
    )
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT email, nome FROM cliente LIMIT 1")
        result = cursor.fetchone()
        if result:
            return result[0], result[1]  # email, nome
        return None, None
    except Exception as e:
        print(f"Erro ao buscar cliente: {e}")
        return None, None
    finally:
        cursor.close()
        conn.close()

def get_first_categoria():
    """Busca a primeira categoria disponível no banco"""
    conn = psycopg2.connect(
        host="localhost", database="techmarket",
        user="techmarket", password="password"
    )
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT DISTINCT categoria FROM produto LIMIT 1")
        result = cursor.fetchone()
        if result:
            return result[0]
        return None
    except Exception as e:
        print(f"Erro ao buscar categoria: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # Buscar cliente e categoria dinamicamente
    print("Buscando primeiro cliente e categoria disponíveis...")
    email_cliente, nome_cliente = get_first_cliente()
    categoria = get_first_categoria()
    
    if not email_cliente:
        print("Nenhum cliente encontrado no banco de dados.")
        exit()
    
    if not categoria:
        print("Nenhuma categoria encontrada no banco de dados.")
        exit()
    
    print(f"Cliente encontrado: {nome_cliente} ({email_cliente})")
    print(f"Categoria encontrada: {categoria}")
    
    # Q1 - Últimos 3 pedidos do primeiro cliente
    run_query("\nQ1 - Últimos 3 pedidos do primeiro cliente encontrado",
              """
              SELECT p.*
              FROM cliente c
              JOIN pedido p ON p.id_cliente = c.id
              WHERE c.email = %s
              ORDER BY p.data_pedido DESC
              LIMIT 3
              """,
              (email_cliente,), formatter=format_row)
   
    # Q2 - Produtos da primeira categoria encontrada
    run_query("\nQ2 - Produtos da primeira categoria encontrada ordenados por preço",
          """
          SELECT * FROM produto
          WHERE categoria = %s
          ORDER BY preco ASC
          """,
          (categoria,), formatter=format_produto)
   
    # Q3 - Pedidos entregues do primeiro cliente
    run_query("\nQ3 - Pedidos entregues do primeiro cliente encontrado",
              """
              SELECT p.*
              FROM cliente c
              JOIN pedido p ON p.id_cliente = c.id
              WHERE c.email = %s AND p.status = 'entregue'
              ORDER BY p.data_pedido DESC
              """,
              (email_cliente,), formatter=format_row)
   
    # Q4 - Top 5 produtos mais vendidos (não precisa de parâmetros dinâmicos)
    run_query("\nQ4 - Top 5 produtos mais vendidos",
              """
              SELECT pr.nome, SUM(ip.quantidade) AS total_vendido
              FROM item_pedido ip
              JOIN produto pr ON pr.id = ip.id_produto
              GROUP BY pr.nome
              ORDER BY total_vendido DESC
              LIMIT 5
              """, formatter=format_mais_vendido)
   
    # Q5 - Pagamentos via PIX no último mês (não precisa de parâmetros dinâmicos)
    run_query("\nQ5 - Pagamentos via PIX no último mês",
              """
              SELECT *
              FROM pagamento
              WHERE tipo = 'pix' AND data_pagamento >= NOW() - INTERVAL '1 month'
              ORDER BY data_pagamento DESC
              """, formatter=format_pagamento)
   
    # Q6 - Total gasto pelo primeiro cliente nos últimos 3 meses
    run_query("\nQ6 - Total gasto pelo primeiro cliente nos últimos 3 meses",
              """
              SELECT c.nome, SUM(p.valor_total) AS total_gasto
              FROM cliente c
              JOIN pedido p ON p.id_cliente = c.id
              WHERE c.email = %s AND p.data_pedido >= NOW() - INTERVAL '3 months'
              GROUP BY c.nome
              """,
              (email_cliente,), formatter=format_total_gasto)