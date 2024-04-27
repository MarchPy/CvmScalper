import os
import wget
import sqlite3
import pandas as pd
from zipfile import ZipFile


class Cvm:
    def __init__(self) -> None:
        self.__data_path = os.path.join(os.path.dirname(p=os.path.dirname(p=__file__)), 'data\\')
        self.__database_path = os.path.join(os.path.dirname(p=os.path.dirname(p=__file__)), 'data\\cvm.db')
        self._start = 2010
        self._end = 2023
        self.__url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
        self.__tmp_path = os.path.join(os.path.dirname(p=os.path.dirname(p=__file__)), '.tmp\\')

    def colect_data(self):
        try:
            os.mkdir(path=self.__tmp_path)

        except FileExistsError:
            pass

        for year in range(self._start, self._end + 1):
            filename = f"dfp_cia_aberta_{year}.zip"
            url = self.__url + filename
            wget.download(url=url, out=os.path.join(self.__tmp_path, filename), bar=False)

    def extract_files(self):
        for year in range(self._start, self._end + 1):
            filename = f"dfp_cia_aberta_{year}.zip"
            file_path = os.path.join(self.__tmp_path, filename)
            with ZipFile(file=file_path) as file_obj:
                file_obj.extractall(path=self.__tmp_path)

            os.remove(path=file_path)

    def remove_files(self):
        for root, _, files in os.walk(top=self.__tmp_path):
            for file in files:
                os.remove(path=os.path.join(root, file))

    def concat_files(self):
        df = pd.DataFrame()

        for root, _, files in os.walk(top=self.__tmp_path):
            for file in files:
                file_path = os.path.join(root, file)
            
            df = pd.concat([df, pd.read_csv(filepath_or_buffer=file_path, sep=';', encoding='iso-8859-1')])


        df = df[df['ORDEM_EXERC'] == 'ÚLTIMO']
        
        if not os.path.exists(path=self.__data_path + "Empresas.csv"):
            try:
                os.mkdir(path=self.__data_path)

            except FileExistsError:
                pass

            df[['DENOM_CIA', 'CD_CVM']].drop_duplicates(subset='CD_CVM').set_index('CD_CVM').to_csv(path_or_buf=os.path.join(self.__data_path, "Empresas.csv"))

        if not os.path.exists(path=self.__data_path + "Descrição de contas.csv"):
            try:
                os.mkdir(path=self.__data_path)

            except FileExistsError:
                pass

            df[['CD_CONTA', 'DS_CONTA']].drop_duplicates(subset='CD_CONTA').set_index('CD_CONTA').to_csv(path_or_buf=os.path.join(self.__data_path, "Descrição de contas.csv"))

        return df
    
    def database(self, df: pd.DataFrame):
        df['DT_FIM_EXERC'] = df['DT_FIM_EXERC'].apply(lambda x: str(x)[:5])

        with sqlite3.connect(database=self.__database_path) as conn:
            cursor = conn.cursor()

            # Corrigindo a sintaxe da criação da tabela
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS dados_cvm (
                    {", ".join([column + " TEXT" for column in df.columns])}
                );
            """
            cursor.execute(create_table_sql)

            # Inserindo os valores do DataFrame na tabela
            for index, row in df.iterrows():
                insert_sql = f"""
                    INSERT INTO dados_cvm ({", ".join(df.columns)})
                    VALUES ({", ".join(["'" + str(value).replace("'", "").replace("/", "") + "'" for value in row])});
                """
                cursor.execute(insert_sql)

    def create_new_file(self):
        with sqlite3.connect(database=self.__database_path) as conn:
            df_db = pd.read_sql(sql="SELECT * FROM dados_cvm", con=conn)
            
        new_df = df_db[['CD_CONTA', 'DS_CONTA']].drop_duplicates().set_index('CD_CONTA').sort_index(ascending=True)
                    
        for index, row in new_df.iterrows():
            for year in range(self._start, self._end + 1):
                new_df[f'-- {year} --'] = 0
                vl_conta = df_db[(df_db['CD_CONTA'] == index) & (df_db['DS_CONTA'] == row['DS_CONTA']) & (df_db['DT_FIM_EXERC'] == year)]['VL_CONTA']
                if not vl_conta.empty:
                    new_df.loc[index, f'-- {year} --'] = vl_conta.iloc[0]

        print(new_df)
