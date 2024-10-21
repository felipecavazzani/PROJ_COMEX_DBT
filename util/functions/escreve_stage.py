import os
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def escreve_dados_stage(df, nome_tabela, schema):
    # Lê as variáveis do ambiente
    usuario = os.getenv('DB_USER')
    senha = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    porta = os.getenv('DB_PORT')
    banco = os.getenv('DB_NAME_COMEX')

    # Verificando as variáveis
    if not all([usuario, senha, host, porta, banco]):
        raise ValueError("Uma ou mais variáveis de ambiente não estão definidas.")

    # Cria a string de conexão
    conexao_str = f'postgresql://{usuario}:{senha}@{host}:{porta}/{banco}'

    try:
        with create_engine(conexao_str).connect() as connection:
            # Grava os dados no schema especificado
            df.to_sql(nome_tabela, connection, schema=schema, if_exists='replace', index=False, chunksize=10000, method='multi')

            # Log de sucesso
            logging.info(f"Dados gravados com sucesso na tabela {schema}.{nome_tabela}")

    except SQLAlchemyError as sql_err:
        # Erros no banco de dados
        logging.error(f"Erro no banco de dados ao gravar os dados na tabela {schema}.{nome_tabela}: {sql_err}")
    except Exception as e:
        # Erros genéricos
        logging.error(f"Ocorreu um erro inesperado ao gravar os dados na tabela {schema}.{nome_tabela}: {e}")
