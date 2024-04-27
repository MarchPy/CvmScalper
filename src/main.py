import os
import wget
import pandas as pd
from zipfile import ZipFile


class Cvm:
    def __init__(self) -> None:
        self.__data_path = os.path.join(os.path.dirname(p=os.path.dirname(p=__file__)), 'data\\')
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
        allowed = ['_DRE_con_', '_BPA_con_', '_BPP_con_']
        for root, _, files in os.walk(top=self.__tmp_path):
            for file in files:
                if not any(alw in file for alw in allowed):
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
