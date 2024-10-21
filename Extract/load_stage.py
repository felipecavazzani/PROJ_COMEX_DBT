import os
import sys

# Diretorio pai 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from util.functions.ler_raw import ler_raw_data
from util.functions.escreve_stage import escreve_dados_stage


def ler_dados_df(file_paths):
    dataframes = {}
    for key, path in file_paths.items():
        try:
            dataframes[key] = ler_raw_data(path)
            print(f"Arquivo {key} com sucesso.")
        except FileNotFoundError:
            print(f"Arquivo {path} não foi encontrado.")
        except Exception as e:
            print(f"Erro: {e}")
    
    return dataframes


def main():
    # caminhos dos arquivos
    file_paths = {
        "imp": r'C:\PROJ_COMEX_DBT\tmp\comex\IMP_2024.csv',
        "sh": r'C:\PROJ_COMEX_DBT\tmp\comex\NCM_SH.csv',
        "ncm": r'C:\PROJ_COMEX_DBT\tmp\comex\NCM.csv',
        "pais": r'C:\PROJ_COMEX_DBT\tmp\comex\PAIS.csv',
        "mun": r'C:\PROJ_COMEX_DBT\tmp\comex\UF_MUN.csv',
        "uf": r'C:\PROJ_COMEX_DBT\tmp\comex\UF.csv',
        "urf": r'C:\PROJ_COMEX_DBT\tmp\comex\URF.csv',
        "via": r'C:\PROJ_COMEX_DBT\tmp\comex\VIA.csv',
    }

    # fazendo a leitura dos df
    dataframes = ler_dados_df(file_paths)

    for key, df in dataframes.items():
        if df is not None:
            try:
                escreve_dados_stage(df, key,'stage')
            except Exception as e:
                print(f"Erro ao gravar o DataFrame '{key}' no banco de dados: {e}")
                

if __name__ == "__main__":
    main()