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
    parser.add_argument('--consulta', type=int, choices= range(1,8), help='Numero da consulta desejada(1-7)')
    return parser.parse_args()


def fazer_consulta1(cursor, pid, pasta):
    print("Q1 - Dado um produto, liste Top 5 comentarios mais uteis e com maior avaliação e os 5 comentarios mais uteis e com menor avaliação:\n")

    try:
        #top 5 com maior avaliação
        #selecionando todos os dados da tabela REVIEW, juntando com a tabela PRODUCT para pegar o Pid, onde o Pid é igual ao dado e a avaliação é 5, ordenando por comentarios uteis de maneira decrescente, limite de 5 comentarios
        comando1 = f"""
            SELECT * FROM REVIEW
            WHERE Pid= {pid} AND rating= 5 
            ORDER BY helpul DESC 
            LIMIT 5 
        """
        cursor.execute(comando1)
        print("Top 5 comentarios mais uteis com maior avaliacao:\n", cursor.fetchall())
        if pasta:
            guardaOutput(cursor, comando1, f"{pasta}/q1_top5_reviews_pos.csv")

        #top 5 com menor avaliação
        #selecionando todos os dados da tabela REVIEW, juntando com a tabela PRODUCT para pegar o Pid, onde o Pid é igual ao dado e a avaliação é 1, ordenando por comentarios uteis de maneira decrescente, limite de 5 comentarios
        comando2 = f"""
            SELECT * FROM REVIEW 
            WHERE Pid= {pid} AND rating= 1 
            ORDER BY helpul DESC
            LIMIT 5
        """
        cursor.execute(comando2)
        print("\nTop 5 comentarios mais uteis com menor avaliacao:\n", cursor.fetchall())
        if pasta:
            guardaOutput(cursor, comando2, f"{pasta}/q1_top5_reviews_neg.csv")

    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível realizar a consulta.", erro)


def fazer_consulta2(cursor, pid, pasta):
    print("Q2 - Dado um produto, listar os produtos similares com maiores vendas (melhor salesrank) do que ele.\n")

    try:
        comando = f"""
            SELECT tab_sim_prod.ASIN_SIM FROM SIMILAR_PRODUCT tab_sim_prod
            JOIN PRODUCT tab_product ON tab_sim_prod.ASIM_SIM= tab_product.ASIN
            JOIN PRODUCT_INFO tab_prod_info ON tab_product.Pid= tab_prod_info.Pid
            WHERE tab_sim_prod.Pid= {pid} AND tab_prod_info.salesrank > (SELECT salesrank FROM PRODUCT_INFO WHERE Pid= {pid})
            ORDER BY tab_prod_info.salesrank DESC
        """
        cursor.execute(comando)
        print("Produtos similares com maiores vendas (melhor salesrank):\n")
        for row in cursor.fetchall():
            print(row)
        if pasta:
            guardaOutput(cursor, comando, f"{pasta}/q2_top_similar_salesrank.csv")
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível realizar a consulta.", erro)


def fazer_consulta3(cursor,pid, pasta):
    print("Q3 - Dado um produto, mostre a evolução diária das médias de avaliação ao longo do período coberto no arquivo\n")

    try:
        #selecionando a data e a media das avaliacoes da tabela REVIEW, onde o Pid é igual ao dado, agrupando por data e ordenando por data
        comando = f"""
            SELECT r.date, AVG(r.rating) as media_diaria_avaliacoes FROM REVIEW r
            WHERE r.Pid= {pid}
            GROUP BY r.date
            ORDER BY r.date
        """
        cursor.execute(comando)
        for row in cursor.fetchall():
            print(f"Data: {row[0]} | Média: {row[1]:.2f}")
        if pasta:
            guardaOutput(cursor, comando, f"{pasta}/q3_avg_evolution.csv")
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível realizar a consulta.", erro)


def fazer_consulta4(cursor, pasta):
    print("Q4 - Listar os 10 produtos líderes de venda em cada grupo de produtos.\n")

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
        print("10 produtos líderes de venda em cada grupo de produtos:\n")
        for row in cursor.fetchall():
            print(row)
        if pasta:
            guardaOutput(cursor, comando, f"{pasta}/q4_top10_salesrank.csv")
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível realizar a consulta.", erro)


def fazer_consulta5(cursor, pasta):
    print('Q5 -Listar os 10 produtos com a maior média de avaliações úteis positivas por produto\n')

    try:
        #agrupando por pid(produto), ordenando por media de avaliacoes uteis de maneira decrescente
        comando = f"""
            SELECT tab_review.Pid, AVG(tab_review.helpul) as media_avaliacoes_uteis
            FROM REVIEW tab_review
            GROUP BY tab_review.Pid
            ORDER BY media_avaliacoes_uteis DESC
            LIMIT 10
        """
        cursor.execute(comando)
        for row in cursor.fetchall():
            print(row)
        if pasta:
            guardaOutput(cursor, comando, f"{pasta}/q5_top10_avg_review_pos.csv")
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível realizar a consulta.", erro)


def fazer_consulta6(cursor, pasta):
    print("Q6 - Listar as 5 categorias com a maior média de avaliações úteis positivas por produto\n")
    
    try:
        comando = f"""
            SELECT tab_category.cat_nome, AVG(tab_review.helpul) as media_avaliacoes_uteis
            FROM REVIEW as tab_review
            JOIN PRODUCT_CAT AS tab_prod_cat ON tab_review.Pid= tab_prod_cat.Pid
            JOIN CATEGORY AS tab_category ON tab_prod_cat.id.final_cat= tab_category.cat_id
            GROUP BY tab_category.cat_nome
            ORDER BY media_avaliacoes_uteis DESC
            LIMIT 5
        """
        cursor.execute(comando)
        for row in cursor.fetchall():
            print(row)
        if pasta:
            guardaOutput(cursor, comando, f"{pasta}/q6_top5_categories_avg_reviews_pos.csv")
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível realizar a consulta.", erro)


def fazer_consulta7(cursor, pasta):
    print("Q7 - Listar os 10 clientes que mais fizeram comentários por grupo de produto\n")

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

        for row in cursor.fetchall():
            print(row)
        if pasta:
            guardaOutput(cursor, comando, f"{pasta}/q7_top10_customer_reviews_pos.csv")
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível realizar a consulta.", erro)

def guardaOutput(cur, arq, consulta):
    try:
        with open(arq, 'w') as arq:
            with cur.copy(f"COPY {consulta} TO STDOUT WITH (FORMAT csv, NULL 'NULL');") as copy:
                for tuplas in copy:
                    arq.write(tuplas)
        print("Resultado da consulta salvo.")
        return 0
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível salvar resultado da consulta.", erro)
        desconecta_cursor(cur)

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
    if args.consulta:
        if args.consulta == 1:
            asin= input("Digite o Pid do produto desejado: ")
            fazer_consulta1(cursor, asin, pasta_output)
        elif args.consulta == 2:
            pasin= input("Digite o Pid do produto desejado: ")
            fazer_consulta2(cursor, pasin, pasta_output)
        elif args.consulta == 3:
            asin= input("Digite o Pid do produto desejado: ")
            fazer_consulta3(cursor, asin, pasta_output)
        elif args.consulta == 4:
            fazer_consulta4(cursor, pasta_output)
        elif args.consulta == 5:
            fazer_consulta5(cursor, pasta_output)
        elif args.consulta == 6:
            fazer_consulta6(cursor, pasta_output)
        elif args.consulta == 7:
            fazer_consulta7(cursor, pasta_output)
    else:
        print("Nenhuma consulta escolhida. Escolha uma consulta com um numero de 1 a 7.")
    
    fim= time.time()
    print(f"\nTempo total de execução: {fim - inicio:.3f} segundos")

    #fecha a conexao com o banco de dados
    desconecta_cursor(cursor)
    desconecta(conexao)

main()
sys.exit(0)