import sys
from db import conecta, desconecta, desconecta_cursor #importando de db.py
import time

def fazer_consulta1(cursor, asin):
    print("Q1 - Dado um produto, liste Top 5 comentarios mais uteis e com maior avaliação e os 5 comentarios mais uteis e com menor avaliação:\n")

    #top 5 com maior avaliação
    #selecionando todos os dados da tabela REVIEW, com avaliacao == 5, seliecionando comentarios uteis decrescente, limite de 5 comentarios
    cursor.execute(f"""
        SELECT * FROM REVIEW 
        WHERE ASIN= {asin} AND rating= 5 
        ORDER BY helpul DESC 
        LIMIT 5 
    """)
    print("Top 5 comentarios mais uteis com maior avaliacao:\n", cursor.fetchall())

    #top 5 com menor avaliação
    ##avaliacao == 1, seliecionando comentarios uteis crescente, limite de 5 comentarios
    cursor.execute(f"""
        SELECT * FROM REVIEW
        WHERE ASIN = {asin} AND rating= 1 
        ORDER BY helpul ASC 
        LIMIT 5
    """)
    print("\nTop 5 comentarios mais uteis com menor avaliacao:\n", cursor.fetchall())

##################
def fazer_consulta2(cursor, pasin):
    print("Q2 - Dado um produto, listar os produtos similares com maiores vendas (melhor salesrank) do que ele.\n")

    ##selecionando todos os produtos similares do produto dado, ordenando por salesrank (posicao de mais vendido) de maneira decrescente
    cursor.execute(f"""
        SELECT ASIN_SIM FROM SIMILAR_PRODUCT 
        WHERE PASIN = {pasin}
        ORDER BY salesrank DESC
    """)
    print("Produtos similares com maiores vendas (melhor salesrank):\n")
    for row in cursor.fetchall():
        print(row)

##################
def fazer_consulta3(cursor,asin):
    print("Dado um produto, mostre a evolução diária das médias de avaliação ao longo do período coberto no arquivo\n")

    #selecionando a data e a media das avaliacoes da tabela REVIEW, agrupando por data e ordenando por data
    cursor.execute(f"""
        SELECT date, AVG(rating) as media_diaria_avaliacoes FROM REVIEW
        WHERE ASIN= {asin}
        GROUP BY date 
        ORDER BY date
    """)
    for row in cursor.fetchall():
        print(f"Data: {row[0]} | Média: {row[1]:.2f}")

def fazer_consulta4(cursor):
    print("Listar os 10 produtos líderes de venda em cada grupo de produtos.\n")

    #selecionando os dados necessarios da tabela PRODUCT pra listar, ordenando por salesrank(mais vendido) de maneira decrescente
    cursor.execute(f"""
        SELECT ASIN, title, p_group, salesrank FROM PRODUCT
        ORDER BY salesrank DESC 
        LIMIT 10         
    """)
    print("10 produtos líderes de venda em cada grupo de produtos:\n")
    for row in cursor.fetchall():
        print(row)

##################
def fazer_consulta5(cursor):
    print('Q5 -Listar os 10 produtos com a maior média de avaliações úteis positivas por produto.')

    #agrupando por ASIN (produto), ordenando por media de avaliacoes uteis de maneira decrescente
    cursor.execute(f"""
        SELECT ASIN, AVG(helpul) as media_avaliacoes_uteis FROM REVIEW
        GROUP BY ASIN 
        ORDER BY media_avaliacoes_uteis DESC 
        LIMIT 10 
    """)
    for row in cursor.fetchall():
        print(row)

##################
def fazer_consulta6(cursor):
    print("Q6 - Listar as 5 categorias com a maior média de avaliações úteis positivas por produto\n")
    #pega ASINs da categoria, pega as reviews desses ASINs, agrupa por categoria, ordena pela media de avaliacoes uteis de maneira decrescente
    cursor.execute(f"""
        SELECT c.cat_nome, AVG(r.helpul) as media_avaliacoes_uteis
        FROM CATEGORY c
        JOIN PRODUCT_CAT PC ON c.cat_id= pc.id_final_cat
        JOIN REVIEW r ON pc.ASIN= r.ASIN
        GROUP BY c.cat_nome
        ORDER BY media_avaliacoes_uteis DESC
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(row)

##################
def fazer_consulta7(cursor):
    print("Q7 - Listar os 10 clientes que mais fizeram comentários por grupo de produto\n")
    cursor.execute(f"""
        SELECT r.cutomer, p.p_group, COUNT(*) as total_comentarios
        FROM REVIEW r
        JOIN PRODUCT p ON r.ASIN= p.ASIN
        GROUP BY r.cutomer, p.p_group
        ORDER BY p.p_group, total_comentarios DESC
        LIMIT 10
    """)
    for row in cursor.fetchall():
        print(row)
##################
#fecha a conexao com o banco de dados
def main():
    conexao = conecta()
    if conexao is None:
        return
    cursor = conexao.cursor()
    if cursor is None:
        desconecta(conexao)
        return

    #medindo o tempo de execução das consultas
    inicio = time.time()
    
    #fazer_consulta1(cursor) #asin do produto
    #fazer_consulta2(cursor) #pasin do produto
    #fazer_consulta3(cursor) #asin do produto
    #fazer_consulta4(cursor)
    #fazer_consulta5(cursor)
    #fazer_consulta6(cursor)
    fazer_consulta7(cursor)

    fim = time.time()
    print("\nTempo de execução da consulta: {fim- inicio:.2f}, segundos")

    desconecta_cursor(cursor)
    desconecta(conexao)