import cassandra
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import time
from datetime import datetime, timedelta
from decimal import Decimal
import uuid 

# --- Funções de Formatação ---
def format_currency_br(valor: Decimal) -> str:
    valor_str = f"{valor:,.2f}"
    return "R$ " + valor_str.replace(",", "X").replace(".", ",").replace("X", ".")

def format_row_pedido(row):
    id_pedido = row.id_pedido
    id_cliente = row.id_cliente
    data_pedido = row.data_pedido
    status = row.status
    valor_total = row.valor_total
    
    data_formatada = data_pedido.strftime('%d/%m/%Y %H:%M') if data_pedido else 'N/A'
    valor_formatado = format_currency_br(valor_total)
    return f"Pedido #{id_pedido} | Cliente #{id_cliente} | Data: {data_formatada} | Status: {status} | Valor: {valor_formatado}"

def format_produto_cassandra(row):
    id_produto = row.id
    nome = row.nome
    categoria = row.categoria
    preco = row.preco
    estoque = row.estoque
    
    preco_formatado = format_currency_br(preco)
    return f"Produto #{id_produto} | Nome: {nome} | Categoria: {categoria} | Preço: {preco_formatado} | Estoque: {estoque}"

def format_mais_vendido_cassandra(row_dict):
    nome = row_dict['nome_produto']
    total_vendido = row_dict['total_vendido']
    return f"Produto: {nome} | Quantidade vendida: {total_vendido}"

def format_pagamento_cassandra(row):
    id_pagamento = row.id 
    id_pedido = row.id_pedido
    tipo = row.tipo
    status = row.status
    data_pagamento = row.data_pagamento
    
    data_formatada = data_pagamento.strftime('%d/%m/%Y %H:%M')
    return f"Pagamento #{id_pagamento} | Pedido #{id_pedido} | Tipo: {tipo.upper()} | Status: {status.capitalize()} | Data: {data_formatada}"

def format_total_gasto_cassandra(row_dict): 
    nome = row_dict['cliente_nome']
    total = row_dict['total_gasto']
    total_formatado = format_currency_br(total)
    return f"Cliente: {nome} | Total gasto: {total_formatado}"

def run_cassandra_query(description, query, params=None, formatter=None, keyspace='techmarket'):
    auth_provider = PlainTextAuthProvider('cassandra', 'cassandra') 
    cluster = Cluster(['localhost'], auth_provider=auth_provider)
    session = cluster.connect(keyspace) 
    
    start = time.time()
    
    try:
        if params:
            rows = session.execute(query, params)
        else:
            rows = session.execute(query)
            
        end = time.time()
        
        print(f"{description} - Tempo: {end - start:.4f} s")
        results_found = False
        for row in rows:
            print(formatter(row) if formatter else row)
            results_found = True
        
        if not results_found:
            print("Nenhum resultado encontrado.")
            
    except Exception as e:
        print(f"Erro ao executar a consulta '{description}': {e}")
    finally:
        session.shutdown()
        cluster.shutdown()

def get_first_cliente_id():
    """Busca o primeiro cliente disponível no banco"""
    auth_provider = PlainTextAuthProvider('cassandra', 'cassandra')
    cluster = Cluster(['localhost'], auth_provider=auth_provider)
    session = cluster.connect('techmarket')
    
    try:
        rows = session.execute("SELECT id, email FROM cliente LIMIT 1")
        for row in rows:
            return row.id, row.email
        return None, None
    except Exception as e:
        print(f"Erro ao buscar cliente: {e}")
        return None, None
    finally:
        session.shutdown()
        cluster.shutdown()

if __name__ == "__main__":
    
    # Buscar um cliente dinamicamente
    print("\nBuscando primeiro cliente disponível...")
    id_cliente_alvo, email_cliente = get_first_cliente_id()
    
    if not id_cliente_alvo:
        print("Nenhum cliente encontrado no banco de dados.")
        exit()
    
    print(f"Cliente encontrado: {email_cliente} (ID: {id_cliente_alvo})")
    
    # Q1 - Últimos 3 pedidos do cliente encontrado
    run_cassandra_query("\nQ1 - Últimos 3 pedidos do cliente",
                        """
                        SELECT id_pedido, id_cliente, data_pedido, status, valor_total
                        FROM pedido_por_cliente
                        WHERE id_cliente = %s
                        LIMIT 3
                        """,
                        (id_cliente_alvo,), formatter=format_row_pedido)
    
    # Q2 - Produtos da categoria Monitores (ou primeira categoria disponível)
    auth_provider = PlainTextAuthProvider('cassandra', 'cassandra')
    cluster = Cluster(['localhost'], auth_provider=auth_provider)
    session = cluster.connect('techmarket')
    
    try:
        # Buscar primeira categoria disponível
        categoria_rows = session.execute("SELECT DISTINCT categoria FROM produto_por_categoria LIMIT 1")
        categoria_alvo = None
        for row in categoria_rows:
            categoria_alvo = row.categoria
            break
            
        if categoria_alvo:
            print(f"\nUsando categoria: {categoria_alvo}")
            start_q2 = time.time()
            q2_rows = session.execute(
                """
                SELECT id, nome, categoria, preco, estoque
                FROM produto_por_categoria
                WHERE categoria = %s
                LIMIT 5
                """,
                (categoria_alvo,)
            )
            end_q2 = time.time()
            
            print(f"Q2 - Produtos da categoria ordenados por preço - Tempo: {end_q2 - start_q2:.4f} s")
            results_found = False
            for row in q2_rows:
                print(format_produto_cassandra(row))
                results_found = True
            
            if not results_found:
                print("Nenhum resultado encontrado.")
        else:
            print("Nenhuma categoria encontrada.")
            
    except Exception as e:
        print(f"Erro ao executar Q2: {e}")
    finally:
        session.shutdown()
        cluster.shutdown()
    
    # Q3 - Pedidos entregues do cliente
    auth_provider = PlainTextAuthProvider('cassandra', 'cassandra')
    cluster = Cluster(['localhost'], auth_provider=auth_provider)
    session = cluster.connect('techmarket')
    
    try:
        start_q3 = time.time()
        q3_query = """
            SELECT id_pedido, id_cliente, data_pedido, status, valor_total
            FROM pedido_por_cliente
            WHERE id_cliente = %s
            LIMIT 10
        """
        q3_rows = session.execute(q3_query, (id_cliente_alvo,))
        
        q3_results = []
        for row in q3_rows:
            if row.status == 'entregue':
                q3_results.append(row)
                
        end_q3 = time.time()
        print(f"\nQ3 - Pedidos entregues do cliente - Tempo: {end_q3 - start_q3:.4f} s")
        if not q3_results:
            print("Nenhum resultado encontrado.")
        for row in q3_results:
            print(format_row_pedido(row))
    except Exception as e:
        print(f"Erro ao executar Q3: {e}")
    finally:
        session.shutdown()
        cluster.shutdown()
 
    # Q4 - Top 5 produtos mais vendidos
    auth_provider = PlainTextAuthProvider('cassandra', 'cassandra')
    cluster = Cluster(['localhost'], auth_provider=auth_provider)
    session = cluster.connect('techmarket')
    
    try:
        start_q4 = time.time()
        all_pedidos = session.execute("SELECT itens FROM pedido_por_cliente LIMIT 100 ALLOW FILTERING;") 
        vendas_por_produto_id = {}
        for pedido_row in all_pedidos:
            if pedido_row.itens: 
                for id_prod, quantidade in pedido_row.itens.items():
                    vendas_por_produto_id[id_prod] = vendas_por_produto_id.get(id_prod, 0) + quantidade
        
        produto_nomes = {}
        if vendas_por_produto_id:
            for prod_id in vendas_por_produto_id.keys():
                try:
                    prod_row = session.execute("SELECT nome FROM produto WHERE id = %s", (prod_id,)).one()
                    if prod_row:
                        produto_nomes[prod_id] = prod_row.nome
                except:
                    continue
        
        final_vendas = []
        for prod_id, total_vendido in vendas_por_produto_id.items():
            if prod_id in produto_nomes:
                final_vendas.append({'nome_produto': produto_nomes[prod_id], 'total_vendido': total_vendido})
        
        sorted_vendas = sorted(final_vendas, key=lambda item: item['total_vendido'], reverse=True)[:5]
        
        end_q4 = time.time()
        print(f"\nQ4 - Top 5 produtos mais vendidos - Tempo: {end_q4 - start_q4:.4f} s")
        if not sorted_vendas:
            print("Nenhum resultado encontrado.")
        for item in sorted_vendas:
            print(format_mais_vendido_cassandra(item))
            
    except Exception as e:
        print(f"Erro ao executar Q4: {e}")
    finally:
        session.shutdown()
        cluster.shutdown()

    # Q5 - Pagamentos via PIX no último mês
    um_mes_atras = datetime.now() - timedelta(days=30)
    
    run_cassandra_query("\nQ5 - Pagamentos via PIX no último mês",
                        """
                        SELECT id, id_pedido, tipo, status, data_pagamento
                        FROM pagamento_por_tipo_data
                        WHERE tipo = %s AND data_pagamento >= %s
                        ORDER BY data_pagamento DESC
                        LIMIT 5;
                        """,
                        ('pix', um_mes_atras), formatter=format_pagamento_cassandra)
    
    # Q6 - Total gasto pelo cliente nos últimos 3 meses
    auth_provider = PlainTextAuthProvider('cassandra', 'cassandra')
    cluster = Cluster(['localhost'], auth_provider=auth_provider)
    session = cluster.connect('techmarket')
    
    try:
        start_q6 = time.time()
        tres_meses_atras = datetime.now() - timedelta(days=90)
        
        q6_query = """
                   SELECT valor_total
                   FROM pedido_por_cliente
                   WHERE id_cliente = %s AND data_pedido >= %s
                   LIMIT 20
                   """
        q6_rows = session.execute(q6_query, (id_cliente_alvo, tres_meses_atras))
        
        total_gasto = Decimal('0.00')
        for row in q6_rows:
            total_gasto += row.valor_total
            
        end_q6 = time.time()
        print(f"\nQ6 - Total gasto pelo cliente nos últimos 3 meses - Tempo: {end_q6 - start_q6:.4f} s")
        print(format_total_gasto_cassandra({'cliente_nome': email_cliente, 'total_gasto': total_gasto}))
    except Exception as e:
        print(f"Erro ao executar Q6: {e}")
    finally:
        session.shutdown()
        cluster.shutdown()