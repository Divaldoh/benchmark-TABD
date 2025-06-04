# init_databases.py
import psycopg2
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time

# PostgreSQL
def init_postgres():
    print("Inicializando PostgreSQL...")
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="techmarket",
        user="techmarket",
        password="password"
    )
    cursor = conn.cursor()

    # Criar tabelas
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cliente (
        id SERIAL PRIMARY KEY,
        nome VARCHAR(100),
        email VARCHAR(100) UNIQUE,
        telefone VARCHAR(20),
        data_cadastro DATE,
        cpf VARCHAR(14)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS produto (
        id SERIAL PRIMARY KEY,
        nome VARCHAR(100),
        categoria VARCHAR(50),
        preco DECIMAL(10, 2),
        estoque INTEGER
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS pedido (
        id SERIAL PRIMARY KEY,
        id_cliente INTEGER REFERENCES cliente(id),
        data_pedido TIMESTAMP,
        status VARCHAR(20),
        valor_total DECIMAL(10, 2)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS item_pedido (
        id_pedido INTEGER REFERENCES pedido(id),
        id_produto INTEGER REFERENCES produto(id),
        quantidade INTEGER,
        PRIMARY KEY (id_pedido, id_produto)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS pagamento (
        id SERIAL PRIMARY KEY,
        id_pedido INTEGER REFERENCES pedido(id),
        tipo VARCHAR(20),
        status VARCHAR(20),
        data_pagamento TIMESTAMP
    );
    """)

    # Criar índices
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cliente_email ON cliente(email);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_produto_categoria ON produto(categoria);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pedido_cliente ON pedido(id_cliente);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pedido_status ON pedido(status);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pagamento_tipo ON pagamento(tipo);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pagamento_data ON pagamento(data_pagamento);")

    conn.commit()
    cursor.close()
    conn.close()
    print("PostgreSQL inicializado com sucesso!")

# MongoDB
def init_mongodb():
    print("Inicializando MongoDB...")
    client = MongoClient('mongodb://techmarket:password@localhost:27017/')
    db = client['techmarket']

    # Verifica se as coleções existem, se não, cria
    colecoes_necessarias = ['clientes', 'produtos', 'pedidos', 'pagamentos']
    colecoes_existentes = db.list_collection_names()

    for colecao in colecoes_necessarias:
        if colecao not in colecoes_existentes:
            db.create_collection(colecao)

    # Criar índices
    db.clientes.create_index('email', unique=True)
    db.produtos.create_index('categoria')
    db.pedidos.create_index('id_cliente')
    db.pedidos.create_index('status')
    db.pagamentos.create_index('tipo')
    db.pagamentos.create_index('data_pagamento')

    print("MongoDB inicializado com sucesso!")

# Cassandra
def init_cassandra():
    print("Inicializando Cassandra...")
    # Aguardar Cassandra iniciar completamente
    time.sleep(30)

    cluster = Cluster(['localhost'])
    session = cluster.connect()

    # Criar keyspace
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS techmarket
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)

    session.set_keyspace('techmarket')

    # Criar tabelas
    session.execute("""
    CREATE TABLE IF NOT EXISTS cliente (
        id UUID PRIMARY KEY,
        nome TEXT,
        email TEXT,
        telefone TEXT,
        data_cadastro DATE,
        cpf TEXT
    );
    """)

    session.execute("""
    CREATE TABLE IF NOT EXISTS produto (
        id UUID PRIMARY KEY,
        nome TEXT,
        categoria TEXT,
        preco DECIMAL,
        estoque INT
    );
    """)

    session.execute("""
    CREATE TABLE IF NOT EXISTS produto_por_categoria (
        categoria TEXT,
        preco DECIMAL,
        id UUID,
        nome TEXT,
        estoque INT,
        PRIMARY KEY (categoria, preco, id)
    );
    """)

    session.execute("""
    CREATE TABLE IF NOT EXISTS pedido_por_cliente (
        id_cliente UUID,
        data_pedido TIMESTAMP,
        id_pedido UUID,
        status TEXT,
        valor_total DECIMAL,
        itens MAP<UUID, INT>,
        PRIMARY KEY (id_cliente, data_pedido, id_pedido)
    ) WITH CLUSTERING ORDER BY (data_pedido DESC);
    """)

    session.execute("""
    CREATE TABLE IF NOT EXISTS pagamento_por_tipo_data (
        tipo TEXT,
        data_pagamento TIMESTAMP,
        id UUID,
        id_pedido UUID,
        status TEXT,
        PRIMARY KEY (tipo, data_pagamento, id)
    );
    """)

    # Criar índices secundários
    session.execute("CREATE INDEX IF NOT EXISTS idx_cliente_email ON cliente(email);")

    print("Cassandra inicializado com sucesso!")

if __name__ == "__main__":
    init_postgres()
    init_mongodb()
    init_cassandra()
    print("Todos os bancos de dados foram inicializados!")