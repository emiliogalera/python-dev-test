import os
import typing
import random
import pandas

from db_handler import DbHandler
from  data_interaction import DataHandler
from data_interaction import HARD_LINE_CONST

# ---- data reading and preparation test ---- #
def instance_test(data_path: str) -> None:
    """Test if instantiation is working properly"""

    dh = DataHandler(data_name=data_path)
    assert type(dh) == DataHandler

def test_load_data(data_path: str) -> None:
    """Test if loading in batches of 1630 is working"""

    dh = DataHandler(data_name=data_path)
    batch = random.randint(0, 19)
    if dh.load(batch):  #Deve falhar apenas no ultimo batch que fica com o que resta dos dados.
        pass
    else:
        raise Exception(f"Should have loaded {HARD_LINE_CONST} lines! Instead got {dh.size}!")

def test_production_data(data_path: str) -> None:
    """The production data adds a bunch of dummy
    columns for categorical data and normalizes
    continuous data. However, the number of rows must be preserved"""

    dh = DataHandler(data_name=data_path)
    dh.load(5)
    dh.treat_missing_data()
    prod_df = dh.production_data()
    assert dh.size == prod_df.shape[0]

def test_make(data_path: str) -> None:
    dh = DataHandler(data_name=data_path)
    prod_df = dh.make(0)
    assert type(prod_df) == pandas.DataFrame
    assert prod_df.shape[0] == dh.size
# ---- ---- #

# ---- database tests ---- #
def connection_test(db_name: str) -> None:
    """Test if a connection is created correctly"""

    db = DbHandler(db_name)
    assert os.path.exists(db_name)

def create_table_test(db_name: str, table_name: str) -> None:
    """Test creating a table and fail to create a table if it already exists."""

    db = DbHandler(db_name)

    if db.create_table(table_name, {"name": str, "age": int}):
        print("Table created!")
    
    if not db.create_table(table_name, {"name": str, "age": int}):
        print("Can't create the same table twice!")
    db.insert(table_name, ("Emilio", 32))



if __name__ == "__main__":

    # test if instantiation is ok
    instance_test("../data/Adult.data")

    # test if the data handler is reading 1630 lines per batch
    test_load_data("../data/Adult.data")
    
    # test if production data is preserving data integrity
    test_production_data("../data/Adult.data")

    # test the make function. It generates data for classification and
    # processing
    test_make("../data/Adult.data")

    connection_test("xxx.sqlite")

    create_table_test("xxx.sqlite", "Hello2")