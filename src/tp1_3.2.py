import sys
import psycopg
import time

def conecta():
    tempo_inicio = time.time()

    try:
        db_host = sys.argv[sys.argv.index('--db-host')+1]
        db_port = sys.argv[sys.argv.index('--db-port')+1]
        db_name = sys.argv[sys.argv.index('--db-name')+1]
        db_user = sys.argv[sys.argv.index('--db-user')+1]
        db_pass = sys.argv[sys.argv.index('--db-pass')+1]
        psycopg.connect(f"host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_pass}")
    except:
        print("Erro")
        return 1
    
    print("Tempo para conexão:", time.time() - tempo_inicio,"seg")
    return 0

arq_i = sys.argv[sys.argv.index('--input')+1]
conecta()