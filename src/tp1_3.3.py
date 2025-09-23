import sys
from db import conecta, desconecta, desconecta_cursor #importando de db.py
import time
import argparse

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


def fazer_consulta1(cursor, pid):
    print("Q1 - Dado um produto, liste Top 5 comentarios mais uteis e com maior avaliação e os 5 comentarios mais uteis e com menor avaliação:\n")

    #top 5 com maior avaliação
    #selecionando todos os dados da tabela REVIEW, juntando com a tabela PRODUCT para pegar o ASIN, onde o ASIN é igual ao dado e a avaliação é 5, ordenando por comentarios uteis de maneira decrescente, limite de 5 comentarios
    cursor.execute(f"""
        SELECT * FROM REVIEW
        WHERE Pid= {pid} AND rating= 5 
        ORDER BY helpul DESC 
        LIMIT 5 
    """)
    print("Top 5 comentarios mais uteis com maior avaliacao:\n", cursor.fetchall())

    #top 5 com menor avaliação
    #selecionando todos os dados da tabela REVIEW, juntando com a tabela PRODUCT para pegar o ASIN, onde o ASIN é igual ao dado e a avaliação é 1, ordenando por comentarios uteis de maneira decrescente, limite de 5 comentarios
    cursor.execute(f"""
        SELECT * FROM REVIEW 
        WHERE Pid= {pid} AND rating= 1 
        ORDER BY helpul DESC
        LIMIT 5
    """)
    print("\nTop 5 comentarios mais uteis com menor avaliacao:\n", cursor.fetchall())


def fazer_consulta2(cursor, pid):
    print("Q2 - Dado um produto, listar os produtos similares com maiores vendas (melhor salesrank) do que ele.\n")

    cursor.execute(f"""
        SELECT tab_sim_prod.ASIN_SIM FROM SIMILAR_PRODUCT tab_sim_prod
        JOIN PRODUCT tab_product ON tab_sim_prod.ASIM_SIM= tab_product.ASIN
        JOIN PRODUCT_INFO tab_prod_info ON tab_product.Pid= tab_prod_info.Pid
        WHERE tab_sim_prod.Pid= {pid} AND tab_prod_info.salesrank > (SELECT salesrank FROM PRODUCT_INFO WHERE Pid= {pid})
        ORDER BY tab_prod_info.salesrank DESC
    """) 
    print("Produtos similares com maiores vendas (melhor salesrank):\n")
    for row in cursor.fetchall():
        print(row)


def fazer_consulta3(cursor,pid):
    print("Dado um produto, mostre a evolução diária das médias de avaliação ao longo do período coberto no arquivo\n")

    #selecionando a data e a media das avaliacoes da tabela REVIEW, onde o Pid é igual ao dado, agrupando por data e ordenando por data
    cursor.execute(f"""
        SELECT r.date, AVG(r.rating) as media_diaria_avaliacoes FROM REVIEW r
        WHERE r.Pid= {pid}
        GROUP BY r.date
        ORDER BY r.date
    """)
    for row in cursor.fetchall():
        print(f"Data: {row[0]} | Média: {row[1]:.2f}")


def fazer_consulta4(cursor):
    print("Listar os 10 produtos líderes de venda em cada grupo de produtos.\n")

    #selecionando os dados necessarios da tabela PRODUCT pra listar, ordenando por salesrank(mais vendido) de maneira decrescente
    cursor.execute(f"""
        SELECT tab_product.ASIN, tab_prod_info.title, tab_prod_info.p_group, tab_prod_info.salesrank 
        FROM PRODUCT tab_product
        JOIN PRODUCT_INFO tab_prod_info ON tab_product.Pid= tab_prod_info.Pid
        ORDER BY tab_prod_info.salesrank DESC
        LIMIT 10
    """)
    print("10 produtos líderes de venda em cada grupo de produtos:\n")
    for row in cursor.fetchall():
        print(row)


def fazer_consulta5(cursor):
    print('Q5 -Listar os 10 produtos com a maior média de avaliações úteis positivas por produto.')

    #agrupando por pid(produto), ordenando por media de avaliacoes uteis de maneira decrescente
    cursor.execute(f"""
        SELECT tab_review.Pid, AVG(tab_review.helpul) as media_avaliacoes_uteis
        FROM REVIEW tab_review
        GROUP BY tab_review.Pid
        ORDER BY media_avaliacoes_uteis DESC
        LIMIT 10
    """)
    for row in cursor.fetchall():
        print(row)


def fazer_consulta6(cursor):
    print("Q6 - Listar as 5 categorias com a maior média de avaliações úteis positivas por produto\n")
    
    cursor.execute(f"""
        SELECT tab_category.cat_nome, AVG(tab_review.helpul) as media_avaliacoes_uteis
        FROM REVIEW as tab_review
        JOIN PRODUCT_CAT AS tab_prod_cat ON tab_review.Pid= tab_prod_cat.Pid
        JOIN CATEGORY AS tab_category ON tab_prod_cat.id.final_cat= tab_category.cat_id
        GROUP BY tab_category.cat_nome
        ORDER BY media_avaliacoes_uteis DESC
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(row)


def fazer_consulta7(cursor):
    print("Q7 - Listar os 10 clientes que mais fizeram comentários por grupo de produto\n")

    #seleciona o cliente, o grupo do produto e a contagem de comentarios, agrupando por cliente e grupo do produto, ordenando por grupo do produto e total de comentarios de maneira decrescente
    cursor.execute(f"""
        SELECT tab_review.cutomer, tab_prod_info.p_group, COUNT(*) as total_comentarios
        FROM REVIEW AS tab_review
        JOIN PRODUCT_INFO AS tab_prod_info ON tab_review.Pid= tab_prod_info.Pid
        GROUP BY tab_review.cutomer, tab_prod_info.p_group
        ORDER BY tab_prod_info.p_group, total_comentarios DESC
        LIMIT 10
    """)
    for row in cursor.fetchall():
        print(row)


#fecha a conexao com o banco de dados
def main():
    args= parse_args()

    conexao= conecta(
        host= args.db_host,
        port= args.db_port,
        dbname= args.db_name,
        user= args.db_user,
        password= args.db_pass
    )
    if conexao is None:
        return 0

    cursor= conexao.cursor()
    if cursor is None:
        desconecta(conexao)
        return 0

    #medir tempo de execucao
    inicio= time.time()

    #executa a consulta escolhida
    if args.consulta:
        if args.consulta == 1:
            asin= input("Digite o Pid do produto desejado: ")
            fazer_consulta1(cursor, asin)
        elif args.consulta == 2:
            pasin= input("Digite o Pid do produto desejado: ")
            fazer_consulta2(cursor, pasin)
        elif args.consulta == 3:
            asin= input("Digite o Pid do produto desejado: ")
            fazer_consulta3(cursor, asin)
        elif args.consulta == 4:
            fazer_consulta4(cursor)
        elif args.consulta == 5:
            fazer_consulta5(cursor)
        elif args.consulta == 6:
            fazer_consulta6(cursor)
        elif args.consulta == 7:
            fazer_consulta7(cursor)
    else:
        print("Nenhuma consulta escolhida. Escolha uma consulta com um numero de 1 a 7.")
    
    fim= time.time()
    print(f"\nTempo total de execução: {fim - inicio:.2f} segundos")

    desconecta_cursor(cursor)
    desconecta(conexao)
