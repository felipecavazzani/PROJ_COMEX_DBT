import os
import requests
import warnings
import datetime

warnings.filterwarnings('ignore')

# temos aqui o ano atual
ano_atual = datetime.datetime.now().year

# URLs
arquivos_para_baixar = [
    f'https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_{ano_atual}.csv',
    'https://balanca.economia.gov.br/balanca/bd/tabelas/UF_MUN.csv',
    'https://balanca.economia.gov.br/balanca/bd/tabelas/UF.csv',
    'https://balanca.economia.gov.br/balanca/bd/tabelas/NCM.csv',
    'https://balanca.economia.gov.br/balanca/bd/tabelas/URF.csv',
    'https://balanca.economia.gov.br/balanca/bd/tabelas/VIA.csv',
    'https://balanca.economia.gov.br/balanca/bd/tabelas/PAIS.csv',
    'https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_SH.csv'
]

# Funcao q baica os arquivos no diretorio definido
def baixar_arquivo(url, caminho_destino):
    
    nome_arquivo = url.split('/')[-1]
    
    # Combina o caminho 
    caminho_completo = os.path.join(caminho_destino, nome_arquivo)
    
    # verifica se existe, se sim ja remove os arquivos
    if os.path.exists(caminho_completo):
        os.remove(caminho_completo)
    
    response = requests.get(url, stream=True, verify=False)
    if response.status_code == 200:
        with open(caminho_completo, "wb") as arquivo:
            for chunk in response.iter_content(chunk_size=128):
                arquivo.write(chunk)
        print(f"Download do  {nome_arquivo} concluído com sucesso.")
    else:
        print(f"Falha ao baixar o arquivo {nome_arquivo}.")

# Define o caminho 
caminho_destino = 'C:/PROJ_COMEX_DBT/tmp/comex'  

# Baixa os arquivos
for url in arquivos_para_baixar:
    baixar_arquivo(url, caminho_destino)

