import sys
import psycopg
import time
import pandas as pd
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
        title VARCHAR(100),
        p_group PRODUCT_GROUP,
        salesrank INT,
        similar INT,
        categories INT,
        reviews INT,
        downloads INT,
        avg_rating INT
        );
        """,
        """CREATE TABLE CATEGORY(
        cat_id INT PRIMARY KEY,
        car_nome VARCHAR(100),
        pcat_id INT,
        pcat_nome VARCHAR(100)
        );
        """,
        """CREATE TABLE PRODUCT_CAT(
        ASIN CHAR(10) PRIMARY KEY REFERENCES PRODUCT(ASIN),
        id_final_cat INT REFERENCES CATEGORY(cat_id)
        );
        """,
        """CREATE TABLE SIMILAR_PRODUCT(
        PASIN CHAR(10) PRIMARY KEY REFERENCES PRODUCT(ASIN),
        ASIN_SIM CHAR(10)
        );
        """,
        """CREATE TABLE PRODUCT_ID(
        ASIN CHAR(10) PRIMARY KEY REFERENCES PRODUCT(ASIN),
        id INT NOT NULL
        );
        """,
        """CREATE TABLE REVIEW(
        ASIN CHAR(10) REFERENCES PRODUCT(ASIN),
        cutomer CHAR(14),
        date DATE,
        rating SMALLINT,
        votes INT,
        helpful INT,
        PRIMARY KEY (ASIN, cutomer),
        CHECK (rating > 0 AND rating <= 5)
        );
        """
    ]
    try:
        for com in comando:
            cur.execute(com)
        print("Tempo para criar o esquema:", time.time() - tempo_inicio,"seg")
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível criar o esquema.", erro)
        return 1
    
    return 0

def lerEntrada():
    tempo_inicio = time.time()
    try:
        arq_i = sys.argv[sys.argv.index('--input')+1]

        print("Tempo para povoar o esquema:", time.time() - tempo_inicio,"seg")

    except ValueError as erro:
        print("Erro na passagem de parâmetros.", erro)
    
    except:
        print("Não foi possível povoar o esquema.")

#Estabelece a conexão com o postgreSQL
conexao = conecta()

if conexao == None:
    sys.exit(1)

cursor = conexao.cursor()

#Criando o esquema e povoando
criaTabelas(cursor)


#Mostrando pra ver se deu certo


#Encerra conexão
desconecta_cursor(cursor)
desconecta(conexao)
sys.exit(0)