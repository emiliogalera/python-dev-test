import typing
import sqlite3

from typing import Dict, Any, Tuple
from sqlite3 import Error

class DbHandler():
    """Class to deal with data base logic.
    Uses sqlite3 currently. Idealy must not include data manipulation logic.
    Creates connection and wrapps some queries"""

    def __init__(self, db_name: str):
        """Initiate the connection with the database"""

        self.connection = sqlite3.connect(db_name)
        self.db_name = db_name
        self.insert_map = {}
    
    def _table_exists(self, tn: str) -> bool:
        """Checks if table tn exists in the current db."""

        cursor = self.connection.cursor()
        res = cursor.execute("""SELECT name FROM sqlite_master WHERE type='table'
                             AND name=?; """, (tn, )).fetchall()
        if res:
            return True
        else:
            return False

    def _make_table_fields(self, cdict: Dict[str, Any]) -> Dict[str, str]:
        """Helper function to parse python types to sqlite3 types"""
        res = {}
        for k, val in cdict.items():
            if val in (int, float):
                res[k] = 'REAL'
            elif val == str:
                res[k] = 'TEXT'
        return res

    def _make_insert_mask(self, table_name: str, columns: Dict[str, Any]) -> None:
        """Creates the basic insert query for insertion on table_name
        param: table_name - The name of the table
        param: columns - used to generate placeholders"""

        base = f"INSERT INTO {table_name}"
        fields = "("
        for k in columns.keys():
            fields += f"{k}, "
        fields = fields[:-2] # removes the last ', '
        fields += ")"

        values = " VALUES("
        for _ in columns.keys():
            values += "?, "
        values = values[:-2] # removes the last ', '
        values += ");"

        self.insert_map[table_name] = base + fields + values


    def create_table(self, table_name: str, columns_dict: Dict[str, Any]) -> bool:
        """Wrapps a query to create a table named table_name with columns columns_dict.
        param:
            table_name - str: name of table to be create.
            columns_dict- dict: {row_name: type}"""
        
        # can't create the same table more than one time
        # but you can create an insert mask
        if self._table_exists(table_name):
            self._make_insert_mask(table_name, columns_dict)
            return False
        else:
            # transform the data type from python to sqlite
            fields = self._make_table_fields(columns_dict) 

            # builds the query to create the table 
            query = f"CREATE TABLE {table_name}"
            query_fiends = "("
            for item, val in fields.items():
                query_fiends += item + " " + val + ", "
            query_fiends = query_fiends[:-2]
            query_fiends += ");"
            query += query_fiends

            # execute the create table command.
            cursor = self.connection.cursor()
            cursor.execute(query)

            # creates the basic string to use when inserting
            self._make_insert_mask(table_name, columns_dict)

            return True

    def insert(self, table_name: str, value_tuple: Tuple[Any], commit: bool=True) -> bool:
        """Wrapps the insert on table query.
        param:
            tabple_name: str - The name of the table to insert"""
        
        if table_name not in self.insert_map:
            raise ValueError(f""" Table {table_name} not in the database.
                             Please create table before inserting""")

        try:
            cursor = self.connection.cursor()
            cursor.execute(self.insert_map[table_name], value_tuple)
        except Error:
            raise Error
        if commit:
            self.connection.commit()
        return True

    def commit(self):
        self.connection.commit()

    def __del__(self):
        self.connection.close()

    @property
    def tables(self):
        """Return the current availuable tables of this database handler"""

        return [k for k in self.insert_map.keys()]