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
        print("Tempo para conexão:", time.time() - tempo_inicio,"seg")

    except ValueError as erro:
        print("Parâmetro exigido faltando.", erro)
        return None
    
    except IndexError as erro:
        print("Valor de parâmetro exigido faltando.")
        return None

    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível conectar-se.", erro)
        return None
    
    return conexao

def desconecta(conexao):
    try:
        conexao.close()
        print("Conexão encerrada.")
    except (psycopg.DatabaseError, Exception) as erro:
        print("Não foi possível encerrar a conexão.", erro)

def desconecta_cursor(cursor):
    try:
        cursor.close()
    except (psycopg.DatabaseError, Exception) as erro:
        print("Erro ao tentar desconectar o cursor.", erro)