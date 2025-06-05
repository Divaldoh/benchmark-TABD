# generate_data.py
import psycopg2
from pymongo import MongoClient
from cassandra.cluster import Cluster
from faker import Faker
import random
import uuid
from datetime import datetime, timedelta
import time
from decimal import Decimal

fake = Faker('pt_BR')

# Configurações
NUM_CLIENTES = 20000
NUM_PRODUTOS = 5000
NUM_PEDIDOS = 30000

# Categorias de produtos
CATEGORIAS = ['Smartphones', 'Notebooks', 'Tablets', 'Acessórios', 'Periféricos',
              'Monitores', 'Smart TVs', 'Áudio', 'Câmeras', 'Games']

# Status de pedidos
STATUS_PEDIDO = ['pendente', 'processando', 'enviado', 'entregue', 'cancelado']

# Tipos de pagamento
TIPOS_PAGAMENTO = ['cartão', 'pix', 'boleto']
STATUS_PAGAMENTO = ['aprovado', 'recusado', 'pendente', 'estornado']

def gerar_clientes(num):
    clientes = []
    for i in range(1, num + 1):
        cliente = {
            'id': i,
            'nome': fake.name(),
            'email': fake.unique.email(),
            'telefone': fake.phone_number(),
            'data_cadastro': fake.date_time_between(start_date='-3y', end_date=datetime.now()),
            'cpf': fake.cpf()
        }
        clientes.append(cliente)
    return clientes

def gerar_produtos(num):
    produtos = []
    for i in range(1, num + 1):
        produto = {
            'id': i,
            'nome': fake.word().capitalize() + ' ' + fake.word().capitalize(),
            'categoria': random.choice(CATEGORIAS),
            'preco': round(random.uniform(10.0, 5000.0), 2),
            'estoque': random.randint(0, 1000)
        }
        produtos.append(produto)
    return produtos

def gerar_pedidos(num, clientes, produtos):
    pedidos = []
    itens_pedido = []

    for i in range(1, num + 1):
        cliente_id = random.choice(clientes)['id']
        data_pedido = fake.date_time_between(start_date='-1y', end_date=datetime.now())
        status = random.choice(STATUS_PEDIDO)

        # Gerar itens do pedido (1 a 5 itens)
        num_itens = random.randint(1, 5)
        produtos_pedido = random.sample(produtos, num_itens)

        valor_total = 0
        for produto in produtos_pedido:
            quantidade = random.randint(1, 3)
            valor_total += produto['preco'] * quantidade

            item = {
                'id_pedido': i,
                'id_produto': produto['id'],
                'quantidade': quantidade
            }
            itens_pedido.append(item)

        pedido = {
            'id': i,
            'id_cliente': cliente_id,
            'data_pedido': data_pedido,
            'status': status,
            'valor_total': round(valor_total, 2),
            'itens': [{'id_produto': item['id_produto'], 'quantidade': item['quantidade']}
                     for item in itens_pedido if item['id_pedido'] == i]
        }
        pedidos.append(pedido)

    return pedidos, itens_pedido

def gerar_pagamentos(pedidos):
    pagamentos = []
    for i, pedido in enumerate(pedidos, 1):
        tipo = random.choice(TIPOS_PAGAMENTO)
        status = random.choice(STATUS_PAGAMENTO)

        # Data de pagamento após a data do pedido (0 a 5 dias depois)
        dias_depois = random.randint(0, 5)
        data_pagamento = pedido['data_pedido'] + timedelta(days=dias_depois)

        pagamento = {
            'id': i,
            'id_pedido': pedido['id'],
            'tipo': tipo,
            'status': status,
            'data_pagamento': data_pagamento
        }
        pagamentos.append(pagamento)

    return pagamentos

# Inserção no PostgreSQL
def inserir_postgres(clientes, produtos, pedidos, itens_pedido, pagamentos):
    print("Inserindo dados no PostgreSQL...")
    start_time = time.time()

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="techmarket",
        user="techmarket",
        password="password"
    )
    cursor = conn.cursor()

    # Limpar dados antigos
    cursor.execute("DELETE FROM pagamento")
    cursor.execute("DELETE FROM item_pedido")
    cursor.execute("DELETE FROM pedido")
    cursor.execute("DELETE FROM produto")
    cursor.execute("DELETE FROM cliente")

    # Inserir clientes
    for cliente in clientes:
        cursor.execute(
            "INSERT INTO cliente (id, nome, email, telefone, data_cadastro, cpf) VALUES (%s, %s, %s, %s, %s, %s)",
            (cliente['id'], cliente['nome'], cliente['email'], cliente['telefone'], cliente['data_cadastro'], cliente['cpf'])
        )

    # Inserir produtos
    for produto in produtos:
        cursor.execute(
            "INSERT INTO produto (id, nome, categoria, preco, estoque) VALUES (%s, %s, %s, %s, %s)",
            (produto['id'], produto['nome'], produto['categoria'], produto['preco'], produto['estoque'])
        )

    # Inserir pedidos
    for pedido in pedidos:
        cursor.execute(
            "INSERT INTO pedido (id, id_cliente, data_pedido, status, valor_total) VALUES (%s, %s, %s, %s, %s)",
            (pedido['id'], pedido['id_cliente'], pedido['data_pedido'], pedido['status'], pedido['valor_total'])
        )

    # Inserir itens de pedido
    for item in itens_pedido:
        cursor.execute(
            "INSERT INTO item_pedido (id_pedido, id_produto, quantidade) VALUES (%s, %s, %s)",
            (item['id_pedido'], item['id_produto'], item['quantidade'])
        )

    # Inserir pagamentos
    for pagamento in pagamentos:
        cursor.execute(
            "INSERT INTO pagamento (id, id_pedido, tipo, status, data_pagamento) VALUES (%s, %s, %s, %s, %s)",
            (pagamento['id'], pagamento['id_pedido'], pagamento['tipo'], pagamento['status'], pagamento['data_pagamento'])
        )

    conn.commit()
    cursor.close()
    conn.close()

    end_time = time.time()
    print(f"Dados inseridos no PostgreSQL em {end_time - start_time:.2f} segundos")
    return end_time - start_time

# Inserção no MongoDB
def inserir_mongodb(clientes, produtos, pedidos, pagamentos):
    print("Inserindo dados no MongoDB...")
    start_time = time.time()

    client = MongoClient('mongodb://techmarket:password@localhost:27017/')
    db = client['techmarket']

    # Limpar dados antigos
    db.clientes.delete_many({})
    db.produtos.delete_many({})
    db.pedidos.delete_many({})
    db.pagamentos.delete_many({})

    # Inserir clientes
    db.clientes.insert_many(clientes)

    # Inserir produtos
    db.produtos.insert_many(produtos)

    # Inserir pedidos (com itens embutidos)
    db.pedidos.insert_many(pedidos)

    # Inserir pagamentos
    db.pagamentos.insert_many(pagamentos)

    end_time = time.time()
    print(f"Dados inseridos no MongoDB em {end_time - start_time:.2f} segundos")
    return end_time - start_time

# Inserção no Cassandra
def inserir_cassandra(clientes, produtos, pedidos, pagamentos):
    print("Inserindo dados no Cassandra...")
    start_time = time.time()

    cluster = Cluster(['localhost'])
    session = cluster.connect('techmarket')

    # Limpar dados antigos
    session.execute("TRUNCATE cliente")
    session.execute("TRUNCATE produto")
    session.execute("TRUNCATE produto_por_categoria")
    session.execute("TRUNCATE pedido_por_cliente")
    session.execute("TRUNCATE pagamento_por_tipo_data")


    # Preparar statements
    insert_cliente = session.prepare(
        "INSERT INTO cliente (id, nome, email, telefone, data_cadastro, cpf) VALUES (?, ?, ?, ?, ?, ?)"
    )

    insert_produto = session.prepare(
        "INSERT INTO produto (id, nome, categoria, preco, estoque) VALUES (?, ?, ?, ?, ?)"
    )

    insert_produto_categoria = session.prepare(
        "INSERT INTO produto_por_categoria (categoria, preco, id, nome, estoque) VALUES (?, ?, ?, ?, ?)"
    )

    insert_pedido = session.prepare(
        "INSERT INTO pedido_por_cliente (id_cliente, data_pedido, id_pedido, status, valor_total, itens) VALUES (?, ?, ?, ?, ?, ?)"
    )

    insert_pagamento = session.prepare(
        "INSERT INTO pagamento_por_tipo_data (tipo, data_pagamento, id, id_pedido, status) VALUES (?, ?, ?, ?, ?)"
    )

    # Inserir clientes
    for cliente in clientes:
        cliente_uuid = uuid.uuid4()
        session.execute(
            insert_cliente,
            (cliente_uuid, cliente['nome'], cliente['email'], cliente['telefone'],
             cliente['data_cadastro'], cliente['cpf'])
        )
        # Mapear ID relacional para UUID para uso posterior
        cliente['uuid'] = cliente_uuid

    # Inserir produtos
    for produto in produtos:
        produto_uuid = uuid.uuid4()
        session.execute(
            insert_produto,
            (produto_uuid, produto['nome'], produto['categoria'],
             Decimal(str(produto['preco'])), produto['estoque'])
        )

        session.execute(
            insert_produto_categoria,
            (produto['categoria'], Decimal(str(produto['preco'])), produto_uuid,
             produto['nome'], produto['estoque'])
        )

        # Mapear ID relacional para UUID
        produto['uuid'] = produto_uuid

    # Criar mapeamento de IDs para UUIDs
    cliente_uuid_map = {c['id']: c['uuid'] for c in clientes}
    produto_uuid_map = {p['id']: p['uuid'] for p in produtos}

    # Inserir pedidos
    for pedido in pedidos:
        pedido_uuid = uuid.uuid4()

        # Converter itens para formato de mapa para Cassandra
        itens_map = {}
        for item in pedido['itens']:
            produto_uuid = produto_uuid_map[item['id_produto']]
            itens_map[produto_uuid] = item['quantidade']

        session.execute(
            insert_pedido,
            (cliente_uuid_map[pedido['id_cliente']], pedido['data_pedido'],
             pedido_uuid, pedido['status'], Decimal(str(pedido['valor_total'])), itens_map)
        )

        # Mapear ID para UUID
        pedido['uuid'] = pedido_uuid

    # Inserir pagamentos
    for pagamento in pagamentos:
        pagamento_uuid = uuid.uuid4()
        pedido_uuid = next(p['uuid'] for p in pedidos if p['id'] == pagamento['id_pedido'])

        session.execute(
            insert_pagamento,
            (pagamento['tipo'], pagamento['data_pagamento'], pagamento_uuid,
             pedido_uuid, pagamento['status'])
        )

    end_time = time.time()
    print(f"Dados inseridos no Cassandra em {end_time - start_time:.2f} segundos")
    return end_time - start_time

if __name__ == "__main__":
    print(f"Gerando {NUM_CLIENTES} clientes...")
    clientes = gerar_clientes(NUM_CLIENTES)

    print(f"Gerando {NUM_PRODUTOS} produtos...")
    produtos = gerar_produtos(NUM_PRODUTOS)

    print(f"Gerando {NUM_PEDIDOS} pedidos...")
    pedidos, itens_pedido = gerar_pedidos(NUM_PEDIDOS, clientes, produtos)

    print(f"Gerando {NUM_PEDIDOS} pagamentos...")
    pagamentos = gerar_pagamentos(pedidos)

    # Inserir dados nos bancos
    tempo_postgres = inserir_postgres(clientes, produtos, pedidos, itens_pedido, pagamentos)
    tempo_mongodb = inserir_mongodb(clientes, produtos, pedidos, pagamentos)
    tempo_cassandra = inserir_cassandra(clientes, produtos, pedidos, pagamentos)

    # Resumo
    print("\nResumo dos tempos de inserção:")
    print(f"PostgreSQL: {tempo_postgres:.2f} segundos")
    print(f"MongoDB: {tempo_mongodb:.2f} segundos")
    print(f"Cassandra: {tempo_cassandra:.2f} segundos")