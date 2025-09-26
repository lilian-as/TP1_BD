import sys
import psycopg
import time
import argparse
from db import * #importando de db.py

def parse_args():
    parser= argparse.ArgumentParser()
    parser.add_argument('--db-host')
    parser.add_argument('--db-port', type=int)
    parser.add_argument('--db-name')
    parser.add_argument('--db-user')
    parser.add_argument('--db-pass')
    parser.add_argument('--output')  #opcional, se quiser salvar resultados
    parser.add_argument('--product-asin')
    return parser.parse_args()

def guardaOutput(cur, conexao, consulta, arq):
    try:
        with open(arq, 'wb') as arq:
            with cur.copy(f"COPY ({consulta}) TO STDOUT WITH (FORMAT csv, NULL 'NULL');") as copy:
                for tuplas in copy:
                    arq.write(tuplas)
        print("Resultado da consulta salvo.")
        return 0
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível salvar resultado da consulta.", erro)
        desconecta_cursor(cur)
        desconecta(conexao)
        sys.exit(1)

def fazer_consulta1(cursor, conexao, asin, pasta):
    print("Dado um produto, lista Top 5 comentarios mais uteis e com maior avaliação e os 5 comentarios mais uteis e com menor avaliação:\n")

    try:
        #top 5 com maior avaliação
        #selecionando todos os dados da tabela REVIEW, juntando com a tabela PRODUCT para pegar o Pid, onde o Pid é igual ao dado e a avaliação é 5, ordenando por comentarios uteis de maneira decrescente, limite de 5 comentarios
        comando1 = f"""
            SELECT DATE,cutomer,rating,votes,helpful FROM REVIEW NATURAL INNER JOIN PRODUCT
            WHERE PRODUCT.ASIN = '{asin}' AND REVIEW.rating >= 4
            ORDER BY helpful DESC 
            LIMIT 5
        """
        cursor.execute(comando1)
        if pasta:
            guardaOutput(cursor, conexao, comando1, f"{pasta}/q1_top5_reviews_pos.csv")
        else:
            print("Top 5 comentarios mais uteis com maior avaliacao:\n", cursor.fetchall())

        #top 5 com menor avaliação
        #selecionando todos os dados da tabela REVIEW, juntando com a tabela PRODUCT para pegar o Pid, onde o Pid é igual ao dado e a avaliação é 1, ordenando por comentarios uteis de maneira decrescente, limite de 5 comentarios
        comando2 = f"""
            SELECT DATE,cutomer,rating,votes,helpful FROM REVIEW NATURAL INNER JOIN PRODUCT
            WHERE PRODUCT.ASIN = '{asin}' AND REVIEW.rating = 1
            ORDER BY helpful DESC 
            LIMIT 5
        """
        cursor.execute(comando2)
        if pasta:
            guardaOutput(cursor, conexao, comando2, f"{pasta}/q1_top5_reviews_neg.csv")
        else:
            print("\nTop 5 comentarios mais uteis com menor avaliacao:\n", cursor.fetchall())

    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível realizar a consulta.", erro)


def fazer_consulta2(cursor, conexao, asin, pasta):
    print("Dado um produto, lista os produtos similares com maiores vendas do que ele.\n")

    try:
        comando = f"""
            SELECT tab_sim_prod.ASIN_SIM FROM SIMILAR_PRODUCT tab_sim_prod
            JOIN PRODUCT p_sim ON p_sim.ASIN = tab_sim_prod.ASIN_SIM
            JOIN PRODUCT_INFO pi_sim ON p_sim.Pid = p_sim.Pid
            WHERE tab_sim_prod.Pid= (SELECT p.Pid FROM PRODUCT p WHERE p.ASIN = '{asin}') AND pi_sim.salesrank > (SELECT pi.salesrank FROM PRODUCT_INFO pi 
            JOIN PRODUCT p ON p.PId= pi.Pid
            WHERE p.ASIN = '{asin}')
            ORDER BY pi_sim.salesrank DESC
        """
        cursor.execute(comando)
        if pasta:
            guardaOutput(cursor, conexao, comando, f"{pasta}/q2_top_similar_salesrank.csv")
        else:
            for row in cursor.fetchall():
                print(row)
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível realizar a consulta.", erro)


def fazer_consulta3(cursor, conexao, asin, pasta):
    print("Dado um produto, mostra a evolução diária das médias de avaliação ao longo do período coberto no arquivo\n")

    try:
        #selecionando a data e a media das avaliacoes da tabela REVIEW, onde o ASIN é igual ao dado, agrupando por data e ordenando por data
        comando = f"""
            SELECT r.date, AVG(r.rating) AS media_diaria_avaliacoes FROM REVIEW r
            JOIN PRODUCT p ON p.Pid = r.Pid
            WHERE p.ASIN = '{asin}'
            GROUP BY r.date
            ORDER BY r.date
        """
        cursor.execute(comando)
        if pasta:
            guardaOutput(cursor, conexao, comando, f"{pasta}/q3_avg_evolution.csv")
        else:
            for row in cursor.fetchall():
                print(f"Data: {row[0]} | Média: {row[1]:.2f}")
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível realizar a consulta.", erro)


def fazer_consulta4(cursor, conexao, pasta):
    print("Lista os 10 produtos líderes de venda em cada grupo de produtos.\n")

    try:
        #selecionando os dados necessarios da tabela PRODUCT pra listar, ordenando por salesrank(mais vendido) de maneira decrescente
        comando = f"""
            SELECT tab_product.ASIN, tab_prod_info.title, tab_prod_info.p_group, tab_prod_info.salesrank 
            FROM PRODUCT tab_product
            JOIN PRODUCT_INFO tab_prod_info ON tab_product.Pid = tab_prod_info.Pid
            ORDER BY tab_prod_info.salesrank DESC
            LIMIT 10
        """
        cursor.execute(comando)
        if pasta:
            guardaOutput(cursor, conexao, comando, f"{pasta}/q4_top10_salesrank.csv")
        else:
            for row in cursor.fetchall():
                print(row)
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível realizar a consulta.", erro)


def fazer_consulta5(cursor, conexao, pasta):
    print('Lista os 10 produtos com a maior média de avaliações úteis positivas por produto\n')

    try:
        #agrupando por pid(produto), ordenando por media de avaliacoes uteis de maneira decrescente
        comando = f"""
            SELECT tab_review.Pid, AVG(tab_review.helpful) as media_avaliacoes_uteis
            FROM REVIEW tab_review
            GROUP BY tab_review.Pid
            ORDER BY media_avaliacoes_uteis DESC
            LIMIT 10
        """
        cursor.execute(comando)
        if pasta:
            guardaOutput(cursor, conexao, comando, f"{pasta}/q5_top10_avg_review_pos.csv")
        else:
            for row in cursor.fetchall():
                print(row)
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível realizar a consulta.", erro)


def fazer_consulta6(cursor, conexao, pasta):
    print("Lista as 5 categorias com a maior média de avaliações úteis positivas por produto\n")
    
    try:
        comando = f"""
            SELECT tab_category.cat_nome, AVG(tab_review.helpful) as media_avaliacoes_uteis
            FROM REVIEW as tab_review
            JOIN PRODUCT_CAT AS tab_prod_cat ON tab_review.Pid= tab_prod_cat.Pid
            JOIN CATEGORY AS tab_category ON tab_prod_cat.id_final_cat= tab_category.cat_id
            GROUP BY tab_category.cat_nome
            ORDER BY media_avaliacoes_uteis DESC
            LIMIT 5
        """
        cursor.execute(comando)
        if pasta:
            guardaOutput(cursor, conexao, comando, f"{pasta}/q6_top5_categories_avg_reviews_pos.csv")
        else:
            for row in cursor.fetchall():
                print(row)
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível realizar a consulta.", erro)


def fazer_consulta7(cursor, conexao, pasta):
    print("Lista os 10 clientes que mais fizeram comentários por grupo de produto\n")

    try:
        #seleciona o cliente, o grupo do produto e a contagem de comentarios, agrupando por cliente e grupo do produto, ordenando por grupo do produto e total de comentarios de maneira decrescente
        comando = f"""
            SELECT tab_review.cutomer, tab_prod_info.p_group, COUNT(*) as total_comentarios
            FROM REVIEW AS tab_review
            JOIN PRODUCT_INFO AS tab_prod_info ON tab_review.Pid= tab_prod_info.Pid
            GROUP BY tab_review.cutomer, tab_prod_info.p_group
            ORDER BY tab_prod_info.p_group, total_comentarios DESC
            LIMIT 10
        """
        cursor.execute(comando)

        if pasta:
            guardaOutput(cursor, conexao, comando, f"{pasta}/q7_top10_customer_reviews_pos.csv")
        else:
            for row in cursor.fetchall():
                print(row)
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível realizar a consulta.", erro)

def main():
    args= parse_args()

    conexao= conecta(args.db_host, args.db_port, args.db_name, args.db_user, args.db_pass)
    if conexao is None:
        sys.exit(1)

    cursor= conexao.cursor()
    if cursor is None:
        desconecta(conexao)
        sys.exit(1)

    #medir tempo de execucao
    inicio= time.time()

    pasta_output = None
    if args.output:
        pasta_output = args.output

    #executa a consulta escolhida
    asin = args.product_asin
    fazer_consulta1(cursor, conexao, asin, pasta_output)
    fazer_consulta2(cursor, conexao, asin, pasta_output)
    fazer_consulta3(cursor, conexao, asin, pasta_output)
    fazer_consulta4(cursor, conexao, pasta_output)
    fazer_consulta5(cursor, conexao, pasta_output)
    fazer_consulta6(cursor, conexao, pasta_output)
    fazer_consulta7(cursor, conexao, pasta_output)
    
    fim= time.time()
    print(f"\nTempo total de execução: {fim - inicio:.3f} segundos")

    #fecha a conexao com o banco de dados
    desconecta_cursor(cursor)
    desconecta(conexao)

main()
sys.exit(0)