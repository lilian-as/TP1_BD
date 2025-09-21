import sys
import psycopg
import time
from db import *

def criaTabelas(cur):
    tempo_inicio = time.time()
    #Criando tabelas de acordo com o esquema

    comando = [
        """
        CREATE DOMAIN PRODUCT_GROUP AS VARCHAR(5) CHECK(value IN ('Book', 'DVD', 'Video', 'Music'));
        """,
        """CREATE TABLE PRODUCT(
        ASIN CHAR(10) PRIMARY KEY,
        title VARCHAR(500),
        p_group PRODUCT_GROUP,
        salesrank INT,
        p_similar INT,
        categories INT,
        reviews INT,
        downloads INT,
        avg_rating INT
        );
        """,
        """CREATE TABLE PRODUCT_ID(
        ASIN CHAR(10) PRIMARY KEY REFERENCES PRODUCT(ASIN),
        Id INT
        PRIMARY KEY (ASIN, Id)
        );
        """,
        """CREATE TABLE SIMILAR_PRODUCT(
        PASIN CHAR(10) REFERENCES PRODUCT(ASIN),
        ASIN_SIM CHAR(10)
        PRIMARY KEY (PASIN, ASIN_SIM)
        );
        """,
        """CREATE TABLE CATEGORY(
        cat_id INT PRIMARY KEY,
        cat_nome VARCHAR(100),
        pcat_id INT REFERENCES CATEGORY(cat_id)
        );
        """,
        """CREATE TABLE PRODUCT_CAT(
        ASIN CHAR(10) REFERENCES PRODUCT(ASIN),
        id_final_cat INT REFERENCES CATEGORY(cat_id)
        PRIMARY KEY (ASIN, id_final_cat)
        );
        """,
        """CREATE TABLE REVIEW(
        ASIN CHAR(10) REFERENCES PRODUCT(ASIN),
        cutomer VARCHAR(14),
        date DATE,
        rating SMALLINT,
        votes INT,
        helpful INT,
        PRIMARY KEY (ASIN, cutomer),
        CHECK (rating > 0 AND rating <= 5)
        CHECK (helpful <= votes)
        );
        """
    ]
    try:
        for com in comando:
            cur.execute(com)
        print("Tempo para criar o esquema:", time.time() - tempo_inicio,"seg")
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível criar o esquema.", erro)
        sys.exit(1)

def insereProduto(cur, asin, titulo, grupo, salesrank, nsimilar, ncategorias, nreviews, ndownloads, avgrating):
    comando = f"""INSERT INTO PRODUCT (ASIN,title,p_group,salesrank,p_similar,categories,reviews,downloads,avg_rating)
                  VALUES ({asin},{titulo},{grupo},{salesrank},{nsimilar},{ncategorias},{nreviews},{ndownloads},{avgrating});"""
    try:
        cur.execute(comando)
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível inserir na relação PRODUCT.", erro)
        sys.exit(1)

def insereProdutoID(cur, id, asin):
    comando = f"INSERT INTO PRODUCT_ID (ASIN,id) VALUES ({asin},{id});"
    try:
        cur.execute(comando)
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível inserir na relação PRODUCT_ID.", erro)
        sys.exit(1)

def insereCategoria(cur, cat_id, cat_nome, pcat_id):
    comando = f"INSERT INTO CATEGORY (cat_id, cat_nome, pcat_id) VALUES ({cat_id},{cat_nome},{pcat_id});"
    try:
        cur.execute(comando)
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível inserir na relação CATEGORY.", erro)
        sys.exit(1)

def insereProdCat(cur, asin, cat_id):
    comando = f"INSERT INTO PRODUCT_CAT (ASIN, id_final_cat) VALUES ({asin},{cat_id});"
    try:
        cur.execute(comando)
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível inserir na relação PRODUCT_CAT.", erro)
        sys.exit(1)

def insereSimilarProd(cur, pasin, asin_sim):
    comando = f"INSERT INTO SIMILAR_PRODUCT (PASIN, ASIN_SIM) VALUES ({pasin},{asin_sim});"
    try:
        cur.execute(comando)
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível inserir na relação SIMILAR_PRODUCT.", erro)
        sys.exit(1)

def insereReview(cur, asin, cutomer, date, rating, votes, helpful):
    comando = f"""INSERT INTO REVIEW (ASIN,cutomer,date,rating,votes,helpful)
                  VALUES ({asin},{cutomer},{date},{rating},{votes},{helpful});"""
    try:
        cur.execute(comando)
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível inserir na relação REVIEW.", erro)
        sys.exit(1)

def lerEntrada(cur):
    tempo_inicio = time.time()
    nlinhas = 0
    #Le arquivo e povoa tabelas
    try:
        arq_i = sys.argv[sys.argv.index('--input')+1]
    except ValueError as erro:
        print("Parâmetro exigido faltanto.", erro)
        sys.exit(1)
    except IndexError:
        print("Valor de parâmetro exigido faltando.")
        sys.exit(1)
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível ler o arquivo.", erro)
        sys.exit(1)

    try:
        with open(arq_i, "r") as arq_i:
            linhas = arq_i.readlines()
        for l in linhas:
            linha = l.split()
            if "Id:" in l:
                id = linha[1]
            elif "ASIN:" in l:
                asin = linha[1]
            elif "title:" in l:
                titulo = l.split(": ", maxsplit=1)[1]
            elif "group:" in l:
                group = linha[1]
            elif "salesrank:" in l:
                sales = linha[1]
            elif "similar:" in l:
                similar = linha[1]
                for asin_sim in linha[2:]:
                    insereSimilarProd(cur, asin, asin_sim)
            elif "categories:" in l:
                cat = linha[1]
            elif linha[0][0] == '|': #Lista de categorias
                linha_cat = l.split('|')
                final = linha_cat[-1]
                id_final_cat = final[(final.index('[')+1):-1]
                insereProdCat(cur, asin, id_final_cat)
                pcat_id = "NULL"
                for cat in linha_cat[1:]:
                    cat_id = cat[(cat.index('[')+1):-1]
                    cat_nome = cat[:(cat.index('['))]
                    insereCategoria(cur, cat_id, cat_nome, pcat_id)
                    pcat_id = cat_id
            elif "reviews:" in l:
                total = l.split(": ")[2].split()[0]
                downloads = l.split(": ")[3].split()[0]
                avg = l.split(": ")[4]
                insereProduto(cur, asin, titulo, group, sales, similar, cat, total, downloads, avg)
                insereProdutoID(cur, id, asin)
            elif "cutomer:" in l:
                data = linha[0]
                cutomer = linha[2]
                rating = linha[4]
                votes = linha[6]
                helpful = linha[8]
                insereReview(cur, asin, cutomer, data, rating, votes, helpful)
            elif "discontinued product" in l:
                insereProduto(cur, asin, "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL")
                insereProdutoID(cur, id, asin)
            nlinhas += 1
            
        print(f"{nlinhas} linhas processadas.")
        print("Tempo para ler o arquivo de entrada e povoar relações:", time.time() - tempo_inicio,"seg")

    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível processar o arquivo.", erro)
        sys.exit(1)

#Estabelece a conexão com o postgreSQL
conexao = conecta()

if conexao == None:
    sys.exit(1)

cursor = conexao.cursor()

#Criando o esquema e povoando
criaTabelas(cursor)
lerEntrada(cursor)

#Encerra conexão
desconecta_cursor(cursor)
desconecta(conexao)
sys.exit(0)