from pymongo import MongoClient
from datetime import datetime, timedelta
from decimal import Decimal

# --- Funções de Formatação (sem alterações) ---
def format_currency_br(valor):
    valor_float = float(valor)
    valor_str = f"{valor_float:,.2f}"
    return "R$ " + valor_str.replace(",", "X").replace(".", ",").replace("X", ".")

def format_row_pedido(row):
    id_pedido = row.get("_id") or row.get("id")
    # Ajuste para pegar o id do cliente de diferentes nomes de 'as' no lookup ou se aninhado
    cliente_info = row.get("cliente_info_docs") or row.get("cliente_info")
    if isinstance(cliente_info, list) and cliente_info: # Se for lista (do $lookup)
        cliente_info = cliente_info[0] # Pega o primeiro dict da lista

    id_cliente = row.get("id_cliente") or (cliente_info.get("id") if isinstance(cliente_info, dict) else None)

    data_pedido = row.get("data_pedido")
    status = row.get("status")
    valor_total = row.get("valor_total")
    if isinstance(data_pedido, datetime):
        data_formatada = data_pedido.strftime('%d/%m/%Y %H:%M')
    else:
        data_formatada = str(data_pedido)
    valor_formatado = format_currency_br(valor_total or 0)

    return f"Pedido #{id_pedido} | Cliente #{id_cliente} | Data: {data_formatada} | Status: {status} | Valor: {valor_formatado}"

def format_produto(row):
    id_produto = row.get("_id") or row.get("id")
    nome = row.get("nome")
    categoria = row.get("categoria")
    preco = row.get("preco")
    estoque = row.get("estoque")
    preco_formatado = format_currency_br(preco or 0)
    return f"Produto #{id_produto} | Nome: {nome} | Categoria: {categoria} | Preço: {preco_formatado} | Estoque: {estoque}"

def format_mais_vendido(row):
    nome = row.get("nome")
    total_vendido = row.get("total_vendido", 0)
    return f"Produto: {nome} | Quantidade vendida: {total_vendido}"

def format_pagamento_pix(result):
    return f"Pedido ID: {result['id_pedido']} | Status: {result['status'].capitalize()} | Data: {result['data_pagamento'].strftime('%d/%m/%Y %H:%M')}"

# --- Função para Executar Consultas ---
def run_mongodb_query(description, collection_name, pipeline, formatter=None):
    client = MongoClient('mongodb://techmarket:password@localhost:27017/')
    db = client.techmarket
    collection = db[collection_name]
    print(description)
    start = datetime.now()
    results = list(collection.aggregate(pipeline))
    end = datetime.now()
    print(f"Tempo: {(end - start).total_seconds():.4f} s")
    if not results:
        print("Nenhum resultado encontrado para esta consulta.")
    for row in results:
        if formatter:
            print(formatter(row))
        elif isinstance(row, dict):
            print(" | ".join(f"{k}: {v}" for k, v in row.items()))
        else:
            print(row)
    print()
    client.close()

# --- Funções para Obter Dados Iniciais ---
def get_first_cliente():
    client = MongoClient('mongodb://techmarket:password@localhost:27017/')
    db = client.techmarket
    cliente = db.clientes.find_one()
    client.close()
    if cliente:
        return cliente.get("email"), cliente.get("nome")
    return None, None

def get_first_categoria():
    client = MongoClient('mongodb://techmarket:password@localhost:27017/')
    db = client.techmarket
    produto = db.produtos.find_one()
    client.close()
    if produto:
        return produto.get("categoria")
    return None

if __name__ == "__main__":
    now = datetime.now()
    email, nome_cliente = get_first_cliente()
    categoria = get_first_categoria()

    if not email:
        print("Nenhum cliente encontrado no banco.")
        exit()
    if not categoria:
        print("Nenhuma categoria encontrada no banco.")
        exit()

    print(f"Usando cliente: {email} ({nome_cliente}) | Categoria: {categoria}")

    # --- Q1 ---
    run_mongodb_query(
        "\nQ1 - Últimos 3 pedidos do primeiro cliente encontrado (máx 5)",
        "pedidos",
        [
            {"$lookup": {"from": "clientes", "localField": "id_cliente", "foreignField": "id", "as": "cliente_info"}},
            {"$unwind": "$cliente_info"},
            {"$match": {"cliente_info.email": email}},
            {"$sort": {"data_pedido": -1}},
            {"$limit": 3}
        ],
        formatter=format_row_pedido
    )

    # --- Q2 ---
    run_mongodb_query(
        "\nQ2 - Produtos da primeira categoria encontrada ordenados por preço (máx 5)",
        "produtos",
        [
            {"$match": {"categoria": categoria}},
            {"$sort": {"preco": 1}},
            {"$limit": 5}
        ],
        formatter=format_produto
    )

    # --- Q3 ---
    run_mongodb_query(
        "\nQ3 - Pedidos entregues do primeiro cliente encontrado (máx 5)",
        "pedidos",
        [
            {"$lookup": {"from": "clientes", "localField": "id_cliente", "foreignField": "id", "as": "cliente_info"}},
            {"$unwind": "$cliente_info"},
            {"$match": {"cliente_info.email": email, "status": "entregue"}},
            {"$sort": {"data_pedido": -1}},
            {"$limit": 5}
        ],
        formatter=format_row_pedido
    )

    # --- Q4 ---
    run_mongodb_query(
        "\nQ4 - Top 5 produtos mais vendidos (com nome) (máx 5)",
        "pedidos",
        [
            {"$unwind": "$itens"},
            {"$group": {"_id": "$itens.id_produto", "total_vendido": {"$sum": "$itens.quantidade"}}},
            {"$sort": {"total_vendido": -1}},
            {"$limit": 5},
            {"$lookup": {"from": "produtos", "localField": "_id", "foreignField": "id", "as": "produto"}},
            {"$unwind": {"path": "$produto", "preserveNullAndEmptyArrays": False}},
            {"$project": {"_id": 0, "nome": "$produto.nome", "total_vendido": 1}}
        ],
        formatter=format_mais_vendido
    )

    # --- Q5 ---
    run_mongodb_query(
        "\nQ5 - Pagamentos via PIX no último mês (máx 5)",
        "pagamentos",
        [
            {"$match": {"tipo": "pix", "data_pagamento": {"$gte": now - timedelta(days=30)}}},
            {"$sort": {"data_pagamento": -1}},
            {"$limit": 5}
        ],
        formatter=format_pagamento_pix
    )

    # --- Consulta Q6 (MODIFICADA para 365 dias) ---
    run_mongodb_query(
        "\nQ6 - Total gasto pelo primeiro cliente encontrado nos últimos 365 dias (máx 5)",
        "pedidos",
        [
            {
                "$lookup": {
                    "from": "clientes",
                    "localField": "id_cliente",
                    "foreignField": "id",
                    "as": "cliente_info"
                }
            },
            {"$unwind": "$cliente_info"},
            {"$match": {
                "cliente_info.email": email,
                "data_pedido": {"$gte": now - timedelta(days=365)} # Período de 365 dias
            }},
            {
                "$group": {
                    "_id": "$cliente_info.nome",
                    "total_gasto": {"$sum": "$valor_total"}
                }
            },
            {"$limit": 5}
        ],
        formatter=lambda row: f"Cliente: {row.get('_id')} | Total gasto: {format_currency_br(row.get('total_gasto', 0))}"
    )
