"""Ferramentas básicas de leitura de dados"""
import os
import typing
import pandas
import numpy as np

from typing import Union, List, Any, Tuple

# nome das colunas obtidos do arquivo data/Description
COLUMN_NAMES = ['age', 'workclass', 'fnlwgt', 'education',
                'education-num', 'marital-status','occupation', 'relationship',
                'race', 'sex', 'capital-gain', 'capital-loss',
                'hours-per-week','native-country', 'class']

CATEGORICAL_COLUMNS = ["workclass", "education", "marital-status", "occupation",
                       "relationship", "race", "sex", "native-country", "class"]

CONTINUOUS_COLUMNS = list(set(COLUMN_NAMES) - set(CATEGORICAL_COLUMNS))

# simbolo utilizado quando falta o valor de um atributo
MISSING_MARKER = "?"

# numero de linhas que podem ser lidos por rodada
HARD_LINE_CONST = 1630

class DataHandler:
    """Classe que lida com leitura de dados"""

    def __init__(self, data_name: str) -> None:
        """A simple wrapper for and empty pandas DataFrame."""

        if not os.path.exists(data_name):
            raise ValueError(f"Could not find the data file!: {data_name}")
        else:
            self.data_file = data_name
            self.loaded = False
            self.df = pandas.DataFrame(columns=COLUMN_NAMES)

    def load(self, delta: int) -> bool:
        """Le as linhas delta*1630 a (delta + 1)*1630.
        Armazena os dados lidos na variável DataFrame"""

        with open(self.data_file, "r") as f:
            counter = 0 # contador de linhas
            start = int(delta*HARD_LINE_CONST) # onde deve começar a ler as linhas da rodada
            stop = HARD_LINE_CONST # onde deve parar de ler as linhas da rodada

            # discarta linhas de 0 até start-1
            if delta > 0:
                carry_line = 0
                for line in f:
                    counter += 1
                    if counter == start:
                        carry_line = line
                        break
                # gambiarra da meia-noite: carry_line é a linha que é lida mas não é processada no loop.
                # discarta a quebra de linha no final da linha
                # faz o split em ',<espaço>' para pegar apenas os valores

                carry_line = carry_line[:-1].split(", ")
                carry_line = self._clean_line(carry_line)
                dummy_df = pandas.DataFrame([carry_line], columns=COLUMN_NAMES)
                self.df = pandas.concat([self.df,
                                             dummy_df],
                                             ignore_index=True)
            counter = 0
            for line in f:
                counter += 1
                splited_line = line[:-1].split(", ")
                splited_line = self._clean_line(splited_line)
                if len(splited_line) == len(COLUMN_NAMES):
                    dummy_df = pandas.DataFrame([splited_line], columns=COLUMN_NAMES)
                    self.df = pandas.concat([self.df,
                                         dummy_df],
                                         ignore_index=True)
                if delta > 0:
                    if counter == stop - 1:
                        break
                else:
                    if counter == stop:
                        break

        if self.df.shape[0] == HARD_LINE_CONST:
            self.loaded = True
            return True
        else:
            return False

    def _clean_line(self, line) -> List[Any]:
        """Exchange '?' for NaN before inserting in the DataFrame.
        This makes it easier to replace missing data down the line"""
        
        ret = []
        for _, val in enumerate(line):
            if val != MISSING_MARKER:
                ret.append(val)
            else:
                ret.append(np.nan)
        return ret

    def treat_missing_data(self) -> bool:
        """Trata dados faltando por substituição do valor ausente pelo
        valor mais frequente da coluna (moda)"""

        if self.df.size != 0:
            self.df = self.df.fillna(self.df.mode().iloc[0])
            return True
        else:
            return False

    def min_max_norm(self, cdf: pandas.DataFrame) -> pandas.DataFrame:
        return (cdf-cdf.min())/(cdf.max()-cdf.min())

    def dummy_encode(self, catdf: pandas.DataFrame) -> pandas.DataFrame:
        return pandas.get_dummies(catdf, columns=catdf.columns, drop_first=True)

    def _clean_non_digit(self, raw_df: pandas.DataFrame) -> pandas.DataFrame:
        """Cleaning some data that should be numeric, but for some reason
        letters are appearing."""

        for row in range(raw_df.shape[0]):
            for column in range(raw_df.shape[1]):
                if not raw_df.iloc[row, column].isdigit():
                    raw_df.iat[row, column] = '0'
        return raw_df


    def production_data(self) -> pandas.DataFrame:
        """Use dummy variables to encode categorical
        columns and max/min normalization on continuous data.
        They are defined in CATEGORICAL_COLUMNS"""

        continuous_df = self._clean_non_digit(self.df.loc[:, CONTINUOUS_COLUMNS]).astype(float)
        categorical_df = self.df.loc[:, CATEGORICAL_COLUMNS]

        normalized_df = self.min_max_norm(continuous_df)
        encoded_df = self.dummy_encode(categorical_df)

        return pandas.concat([normalized_df, encoded_df], axis=1)

    def make(self, batch: int) -> Tuple[bool, pandas.DataFrame]:
        """Run through the process of extraction and transforming the data to
        production"""

        flag = self.load(batch)
        self.treat_missing_data()
        return flag, self.production_data()

    @property
    def current_data(self) -> pandas.DataFrame:
        """Propriedade que pega o DataFrame dos dados correntes.
        Usado mais para debug, mas útil."""

        return self.df
    
    @property
    def size(self) -> int:
        """Retorna o número de colunas que o DataFrame atual tem"""
        return self.df.shape[0]

