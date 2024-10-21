#import pandas as pd
import pandas as pd

def ler_raw_data(origem):
    try:
        df = pd.read_csv(origem, sep=';', encoding='ISO-8859-1')
        return df
    except FileNotFoundError:
        print(f"O arquivo {origem} não foi encontrado.")
        return None
    except Exception as e:
        print(f"Ocorreu um erro ao ler o arquivo: {e}")
        return None
