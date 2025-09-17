import sys
import psycopg
import time

def conecta():
    tempo_inicio = time.time()
    conexao = None
    try:
        db_host = sys.argv[sys.argv.index('--db-host')+1]
        db_port = sys.argv[sys.argv.index('--db-port')+1]
        db_name = sys.argv[sys.argv.index('--db-name')+1]
        db_user = sys.argv[sys.argv.index('--db-user')+1]
        db_pass = sys.argv[sys.argv.index('--db-pass')+1]
        conexao = psycopg.connect(f"host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_pass}")
    except:
        print("Erro")
        return conexao, 1
    
    print("Tempo para conexão:", time.time() - tempo_inicio,"seg")
    return conexao, 0

def criaTabelas(cur):

    #Criando tabelas de acordo com o esquema

    cur.execute("""CREATE TABLE Product(
                ASIN CHAR(10) PRIMARY KEY,
                title VARCHAR(100),
                p_group VARCHAR(5),
                salesrank INT,
                similar INT, 
                categories INT,
                reviews INT,
                downloads INT,
                avg_rating INT,
                );
                """)
    
    cur.execute("""CREATE TABLE Category(
                cat_id INT PRIMARY KEY,
                car_nome VARCHAR(100),
                pcat_id INT,
                pcat_nome VARCHAR(100)
                );
                """)
    
    cur.execute("""CREATE TABLE Product_cat(
                ASIN CHAR(10) PRIMARY KEY REFERENCES Product(ASIN),
                id_final_cat INT REFERENCES Category(cat_id)
                );
                """)
    
    cur.execute("""CREATE TABLE Similar_Product(
                PASIN CHAR(10) PRIMARY KEY REFERENCES Product(ASIN),
                ASIN_SIM CHAR(10)
                );
                """)
    
    cur.execute("""CREATE TABLE Product_id(
                ASIN CHAR(10) PRIMARY KEY REFERENCES Product(ASIN),
                id INT NOT NULL
                );
                """)
    
    cur.execute("""CREATE TABLE Review(
                ASIN CHAR(10) REFERENCES Product(ASIN),
                cutomer CHAR(14),
                date DATE,
                rating SMALLINT,
                votes INT,
                helpful INT,
                PRIMARY KEY (ASIN, cutomer),
                CHECK (rating > 0 AND rating <= 5)
                );
                """)

arq_i = sys.argv[sys.argv.index('--input')+1]

#Estabelece a conexão com o postgreSQL
conexao, status = conecta()
cursor = conexao.cursor()

#Criando tabelas
criaTabelas(cursor)