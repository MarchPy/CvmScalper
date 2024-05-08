import os
import wget
import sqlite3
import requests
import pandas as pd
from numpy import float64
from zipfile import ZipFile

class CVM:
    def __init__(self):
        self.__url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"
        self.__start_year = 2023
        self.__end_year = 2024
        self.__tmp_path = os.path.join(os.path.dirname(p=__file__), '.tmp/')
        self.__database = os.path.join(os.path.dirname(p=__file__), 'data/')

    def colect_data(self):
        try:
            os.mkdir(path=self.__tmp_path)
        except FileExistsError:
            pass
        
        for year in range(self.__start_year, self.__end_year + 1):
            filename = f"itr_cia_aberta_{year}.zip"
            url = self.__url + filename
            out = os.path.join(self.__tmp_path, filename)
            wget.download(url=url, out=out)

    def transform_data(self):
        # Extraindo e removendo os arquivos .zip
        for file in os.listdir(path=self.__tmp_path):
            filepath = os.path.join(self.__tmp_path, file)
            ZipFile(file=filepath).extractall(path=self.__tmp_path)
            os.remove(path=filepath)
        
        # Filtrando e apagandos os arquivos desnecessários
        allowed = ['BPA_con', 'BPP_con']
        files = os.listdir(path=self.__tmp_path)
        filtered_files = [file for file in files for allw in allowed if allw in file]
        for file in os.listdir(path=self.__tmp_path):
            if file not in filtered_files:
                os.remove(path=os.path.join(self.__tmp_path, file))

        # Concatenando e tratando arquivos
        df = pd.DataFrame()
        for file in os.listdir(path=self.__tmp_path):
            filepath = os.path.join(self.__tmp_path, file)
            df_tmp = pd.read_csv(filepath_or_buffer=filepath, sep=';', encoding='iso-8859-1')
            df = pd.concat([df, df_tmp])
            os.remove(path=filepath)

        os.rmdir(path=self.__tmp_path)

        df = df[df['ORDEM_EXERC'] == 'ÚLTIMO']
        df['DT_FIM_EXERC'] = df['DT_FIM_EXERC'].apply(func=lambda x: x[:7])
        print(len(df))
        return df

    def database(self, df: pd.DataFrame):
        try:
            os.mkdir(path=self.__database)
        
        except FileExistsError:
            pass

        with sqlite3.connect(database=os.path.join(self.__database, 'cvm.db')) as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS dados_cvm (
                {", ".join([f"{column} TEXT" for column in df.columns])}
            );
            """)
            
            for index, row in df.iterrows():
                cursor.execute(f"INSERT INTO dados_cvm VALUES ({', '.join(['?' for _ in df.columns])})", tuple(row))

    def fetch_data(self, cd_cvm: str):
        with sqlite3.connect(database=os.path.join(self.__database, 'cvm.db'))as conn:
            df_from_database = pd.read_sql_query(sql="SELECT * FROM dados_cvm", con=conn)

        df_from_database[['DENOM_CIA', 'CD_CVM']].drop_duplicates(subset='CD_CVM').to_excel("empresas.xlsx", index=False)

        nome = df_from_database[df_from_database['CD_CVM'] == cd_cvm]['DENOM_CIA'].values[0]
        df_empresa = df_from_database[df_from_database['CD_CVM'] == cd_cvm][['CD_CONTA', 'DS_CONTA']].drop_duplicates().set_index(keys='DS_CONTA')
        
        for index, row in df_empresa.iterrows():
            for year in range(self.__start_year, self.__end_year + 1):
                for month in range(3, 12, 3):
                    try:
                        value = df_from_database[
                            (df_from_database['CD_CVM'] == cd_cvm) &
                            (df_from_database['DS_CONTA'] == index) &
                            (df_from_database['CD_CONTA'] == row['CD_CONTA']) &
                            (df_from_database['DT_FIM_EXERC'] == f"{year}-0{month}")
                        ]['VL_CONTA'].values[0]
                        df_empresa.loc[index, f'{year}-0{month}'] = value

                    except IndexError:
                        df_empresa.loc[index, f'{year}-0{month}'] = 0
                        
        df_empresa.to_excel(f"{nome.replace('/', '').replace('.', '')}.xlsx")
        
            


app = CVM()
app.colect_data()
df = app.transform_data()
app.database(df=df)
app.fetch_data(cd_cvm=input("Digita o código CVM >>> "))
