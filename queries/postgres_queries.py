import psycopg2
import time
from datetime import datetime
from decimal import Decimal

def format_currency_br(valor: Decimal) -> str:
    valor_str = f"{valor:,.2f}"  # Ex: 34167.05 → '34,167.05'
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
    for row in results:
        print(formatter(row) if formatter else row)
    cursor.close()
    conn.close()

if __name__ == "__main__":
    run_query("\nQ1 - Últimos 3 pedidos por email",
              """
              SELECT p.*
              FROM cliente c
              JOIN pedido p ON p.id_cliente = c.id
              WHERE c.email = %s
              ORDER BY p.data_pedido DESC
              LIMIT 3
              """,
              ('bjesus@example.net',))
    
    run_query("\nQ2 - Produtos da categoria ordenados por preço",
          """
          SELECT * FROM produto
          WHERE categoria = %s
          ORDER BY preco ASC
          """,
          ('Monitores',),
          formatter=format_produto)
    
    run_query("\nQ3 - Pedidos entregues de um cliente",
              """
              SELECT p.*
              FROM cliente c
              JOIN pedido p ON p.id_cliente = c.id
              WHERE c.email = %s AND p.status = 'entregue'
              ORDER BY p.data_pedido DESC
              """,
              ('bjesus@example.net',))
    
    run_query("\nQ4 - Top 5 produtos mais vendidos",
              """
              SELECT pr.nome, SUM(ip.quantidade) AS total_vendido
              FROM item_pedido ip
              JOIN produto pr ON pr.id = ip.id_produto
              GROUP BY pr.nome
              ORDER BY total_vendido DESC
              LIMIT 5
              """)
    
    run_query("\nQ5 - Pagamentos via PIX no último mês",
              """
              SELECT *
              FROM pagamento
              WHERE tipo = 'pix' AND data_pagamento >= NOW() - INTERVAL '1 month'
              ORDER BY data_pagamento DESC
              """)
    
    run_query("\nQ6 - Total gasto por cliente nos últimos 3 meses",
              """
              SELECT c.nome, SUM(p.valor_total) AS total_gasto
              FROM cliente c
              JOIN pedido p ON p.id_cliente = c.id
              WHERE c.email = %s AND p.data_pedido >= NOW() - INTERVAL '3 months'
              GROUP BY c.nome
              """,
              ('bjesus@example.net',))

