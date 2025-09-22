import sys
import psycopg
import time
from db import *
import traceback

def criaTabelas(cur):
    tempo_inicio = time.time()
    #Criando tabelas de acordo com o esquema

    comando = [
        """CREATE TABLE PRODUCT(
        Pid INT PRIMARY KEY,
        ASIN CHAR(10) NOT NULL UNIQUE
        );
        """,
        """CREATE TABLE PRODUCT_INFO(
        Pid INT PRIMARY KEY REFERENCES PRODUCT(Pid),
        title VARCHAR(500),
        p_group VARCHAR(10),
        salesrank INT,
        p_similar INT,
        categories INT,
        reviews INT,
        downloads INT,
        avg_rating FLOAT
        );
        """,
        """CREATE TABLE SIMILAR_PRODUCT(
        Pid INT REFERENCES PRODUCT(Pid),
        ASIN_SIM CHAR(10),
        PRIMARY KEY (Pid, ASIN_SIM)
        );
        """,
        """CREATE TABLE CATEGORY(
        cat_id INT PRIMARY KEY,
        cat_nome VARCHAR(100),
        pcat_id INT REFERENCES CATEGORY(cat_id),
        UNIQUE (cat_id,cat_nome)
        );
        """,
        """CREATE TABLE PRODUCT_CAT(
        Pid INT REFERENCES PRODUCT(Pid),
        id_final_cat INT REFERENCES CATEGORY(cat_id),
        PRIMARY KEY (Pid, id_final_cat)
        );
        """,
        """CREATE TABLE REVIEW(
        rev_id INT,
        Pid INT REFERENCES PRODUCT(Pid),
        cutomer VARCHAR(14),
        date DATE,
        rating SMALLINT,
        votes INT,
        helpful INT,
        PRIMARY KEY (rev_id, Pid, cutomer),
        CHECK (rating > 0 AND rating <= 5),
        CHECK (helpful <= votes)
        );
        """
    ]
    try:
        for com in comando:
            cur.execute(com)
        print("Tempo para criar o esquema:", time.time() - tempo_inicio,"seg")
        return 0
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível criar o esquema.", erro)
    return 1

def insereProduto(cur, id, asin):
    comando = f"INSERT INTO PRODUCT (Pid,ASIN) VALUES ({id},'{asin}');"
    try:
        cur.execute(comando)
    except (psycopg.DatabaseError, Exception) as erro:
        print("Operação", comando, "falhou.", erro)
        desconecta_cursor(cursor)
        desconecta(conexao)
        sys.exit(1)

def insereProdInfo(cur, id, titulo, grupo, salesrank, nsimilar, ncategorias, nreviews, ndownloads, avgrating):
    comando = f"""INSERT INTO PRODUCT_INFO (Pid,title,p_group,salesrank,p_similar,categories,reviews,downloads,avg_rating)
                  VALUES ({id},'{titulo}','{grupo}',{salesrank},{nsimilar},{ncategorias},{nreviews},{ndownloads},{avgrating});"""
    try:
        cur.execute(comando)
    except (psycopg.DatabaseError, Exception) as erro:
        print("Operação", comando, "falhou.", erro)
        desconecta_cursor(cursor)
        desconecta(conexao)
        sys.exit(1)

def insereSimilarProd(cur, id, asin_sim):
    comando = f"INSERT INTO SIMILAR_PRODUCT (Pid, ASIN_SIM) VALUES ({id},'{asin_sim}');"
    try:
        cur.execute(comando)
    except (psycopg.DatabaseError, Exception) as erro:
        print("Operação", comando, "falhou.", erro)
        desconecta_cursor(cursor)
        desconecta(conexao)
        sys.exit(1)

def insereCategoria(cur, cat_id, cat_nome, pcat_id):
    comando = f"""INSERT INTO CATEGORY (cat_id, cat_nome, pcat_id) VALUES ({cat_id},'{cat_nome}',{pcat_id})
                  ON CONFLICT (cat_id,cat_nome) DO NOTHING;"""
    try:
        cur.execute(comando)
    except (psycopg.DatabaseError, Exception) as erro:
        print("Operação", comando, "falhou.", erro)
        desconecta_cursor(cursor)
        desconecta(conexao)
        sys.exit(1)

def insereProdCat(cur, id, cat_id):
    comando = f"INSERT INTO PRODUCT_CAT (Pid, id_final_cat) VALUES ({id},{cat_id});"
    try:
        cur.execute(comando)
    except (psycopg.DatabaseError, Exception) as erro:
        print("Operação", comando, "falhou.", erro)
        desconecta_cursor(cursor)
        desconecta(conexao)
        sys.exit(1)

def insereReview(cur, rev_id, id, cutomer, date, rating, votes, helpful):
    comando = f"""INSERT INTO REVIEW (rev_id,Pid,cutomer,date,rating,votes,helpful)
                  VALUES ({rev_id},{id},'{cutomer}','{date}',{rating},{votes},{helpful});"""
    try:
        cur.execute(comando)
    except (psycopg.DatabaseError, Exception) as erro:
        print("Operação", comando, "falhou.", erro)
        desconecta_cursor(cursor)
        desconecta(conexao)
        sys.exit(1)

def lerEntrada():
    tempo_inicio = time.time()
    linhas = ''
    try:
        arq_i = sys.argv[sys.argv.index('--input')+1]
        with open(arq_i, "r") as arq_i:
            linhas = arq_i.readlines()
        print("Tempo para ler o arquivo de entrada:", time.time() - tempo_inicio,"seg")
    except ValueError as erro:
        print("Parâmetro exigido faltanto.", erro)
    except IndexError:
        print("Valor de parâmetro exigido faltando.")
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível ler o arquivo.", erro)
    return linhas

def povoaRelacoes(cur, linhas):
    tempo_inicio = time.time()
    nlinhas = 0
    try:
        for l in linhas:
            linha = l.split()
            if "Id:   " in l:
                id = linha[1]
            elif "ASIN:" in l:
                asin = linha[1]
                insereProduto(cur, id, asin)
            elif "title:" in l:
                titulo_cru = l.split(": ", maxsplit=1)[1]
                if "'" not in titulo_cru:
                    titulo = titulo_cru
                else:
                    titulo = ''
                    for i in titulo_cru:
                        if i != "'":
                            titulo += i
                        else:
                            titulo += "''"
            elif "group:" in l:
                group = linha[1]
            elif "salesrank:" in l:
                sales = linha[1]
            elif "similar:" in l:
                similar = linha[1]
                for asin_sim in linha[2:]:
                    insereSimilarProd(cur, id, asin_sim)
            elif "categories:" in l:
                categoria = linha[1]
            elif '|' in l and linha[0][0] == '|': #Lista de categorias
                linha_cat = l.split('|')
                pcat_id = "NULL"
                for cat in linha_cat[1:]:
                    cat = cat.replace('\n','')
                    if cat.index(']') == len(cat)-1:
                        cat_id = cat[(cat.index('[')+1):(cat.index(']'))]
                        cat_nome_cru = cat[:(cat.index('['))]
                    else:
                        cat_id = cat[(cat.index('[')+1):(len(cat)-1)]
                        i_cochete = cat_id.index('[')
                        cat_id = cat_id[i_cochete+1:]
                        i_cochete += cat.index('[')
                        cat_nome_cru = cat[:i_cochete+1]
                    if "'" not in cat_nome_cru:
                        cat_nome = cat_nome_cru
                    else:
                        cat_nome = ''
                        for i in cat_nome_cru:
                            if i != "'":
                                cat_nome += i
                            else:
                                cat_nome += "''"
                    insereCategoria(cur, cat_id, cat_nome, pcat_id)
                    pcat_id = cat_id
                insereProdCat(cur, id, cat_id)
            elif "reviews:" in l:
                total = l.split(": ")[2].split()[0]
                downloads = l.split(": ")[3].split()[0]
                avg = l.split(": ")[4]
                insereProdInfo(cur, id, titulo, group, sales, similar, categoria, total, downloads, avg)
                rev_id = 1
            elif "cutomer:" in l:
                data = linha[0]
                cutomer = linha[2]
                rating = linha[4]
                votes = linha[6]
                helpful = linha[8]
                insereReview(cur, rev_id, id, cutomer, data, rating, votes, helpful)
                rev_id += 1
            nlinhas += 1
            
        print(f"{nlinhas} linhas processadas.")
        print("Tempo para povoar relações:", time.time() - tempo_inicio,"seg")
        return 0
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível povoar relações.", erro)
    return 1

#Estabelece a conexão com o postgreSQL
conexao = conecta()
if conexao == None:
    sys.exit(1)
cursor = conexao.cursor()

#Criando o esquema e povoando
status = criaTabelas(cursor)
if status == 0:
    entrada = lerEntrada()
    status = povoaRelacoes(cursor, entrada)

#Encerra conexão
desconecta_cursor(cursor)
desconecta(conexao)
if status != 0:
    sys.exit(1)
sys.exit(0)