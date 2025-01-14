import pandas as pd
                                                        
def tbl_canais(path_filename: str) -> pd.DataFrame:
    df_canais = pd.read_excel(path_filename, engine='openpyxl', sheet_name='Planilha1')
    df_canais.rename(columns=lambda x: x.lower(),inplace=True)
    df_canais.fillna(value='', inplace=True)
    
    return df_canais

def tbl_expansao(path_filename: str) -> pd.DataFrame:
    df_expansao = pd.read_excel(path_filename
                                , engine='openpyxl'
                                , sheet_name='default_1'
                                , dtype={'COD_IBGE': int, 'QTD': int})
    df_expansao.rename(columns=lambda x: x.lower(),inplace=True)
    df_expansao.fillna(value='', inplace=True)
    
    return df_expansao 
