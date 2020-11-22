import psycopg2
import xlrd
from decouple import config

database = config('DB_NAME')
user = config('DB_USER')
password = config('DB_PASSWORD')
host = config('DB_HOST')
port = config('DB_PORT')
filename = config('FILENAME')


class InteractPsql:
    def __init__(self, database, user, password, host, port):
        """Improved and faster custom interaction with postgresql

        Args:
            database (string): database name
            user (string): user name
            password (string): password
            host (string): host name
            port (string): port number
        """
        self._pg_db = psycopg2.connect(database=database, user=user,
                                       password=password, host=host, port=port)
        self._read_query = "SELECT * FROM {0}"
        self._insert_query = """INSERT INTO {0} ({1}) VALUES ({2}) RETURNING *"""
        self._cursor = self._pg_db.cursor()

    def get_pg_db(self):
        """Get the object of connection to the database

        Returns:
            object: connection to the database psycopg2.connect
        """
        return self._pg_db

    def get_cursor(self):
        """Get the cursor object to perform database operations

        Returns:
            object: cursor
        """
        return self._cursor

    def custom_read_query(self, query_string):
        """Reads from database with the string and returns all

        Args:
            query_string (string): full query string to pass to obtain results from the database

        Returns:
            list: results
        """
        self._cursor.execute(query_string)
        records = self._cursor.fetchall()
        return records

    def custom_save_query(self, query_string):
        """Saves to the database with the string. Doesn't return anything

        Args:
            query_string (string): full query to pass to save into database
        """
        self._cursor.execute(query_string)
        self._cursor.commit()

    def custom_save_query_return(self, query_string):
        """Saves to the database with the string. And return all

        Args:
            query_string (string): full query to pass to save into database
        """
        self._cursor.execute(query_string)
        self._cursor.commit()
        records = self._cursor.fetchall()
        return records

    def get_raw_columns_info(self, table_name):
        """Get raw info about columns

        Args:
            table_name (string): name of the table

        Returns:
            list: list of tuples with columns info
        """
        get_col_query = self._read_query.format(
            "information_schema.columns") + """ WHERE table_name = '{0}'""".format(table_name)
        self._cursor.execute(get_col_query)
        records = self._cursor.fetchall()
        return records

    def print_raw_columns_info(self, table_name):
        """Print raw info about columns

        Args:
            table_name (string): The name of the table
        """
        print(self.get_raw_columns_info(table_name))

    def read_all_from_psql(self, table_name):
        """Return all the records from the table

        Args:
            table_name (string): name of the table

        Returns:
            list: table where values are encapsulated in tuples
        """
        self._cursor.execute(self._read_query.format(table_name))
        records = self._cursor.fetchall()
        return records

    def print_raw_all_from_psql(self, table_name):
        """Print all the records from the table

        Args:
            table_name (string): name of the table
        """
        print(self.read_all_from_psql(table_name))

    def save_match_to_psql(self, table_name, column_list, values_insert):
        """Save to postgresql database with the requirement that the all the values must be the same length of columns

        Args:
            table_name (string): name of the table
            column_list (tuple or list): tuple or list of the names of the columns (string) to insert
            values_insert (list): list of tuples (which must have the same length as the tuple from column_list) with the values to insert
        """
        all_val_list_or_tuple = all(isinstance(
            value, (list, tuple)) for value in values_insert)
        all_len_match = all(len(value) == len(column_list)
                            for value in values_insert)
        column_string = ", ".join(column_list)
        if (all_len_match and all_val_list_or_tuple):
            for value in values_insert:
                value_string = ", ".join(value)
                self._cursor.execute(self._insert_query.format(
                    table_name, column_string, value_string))
                self._pg_db.commit()
                records = self._cursor.fetchall()
                print("Output: ", records)
        elif (all_val_list_or_tuple == False and (len(column_list) == len(values_insert))):
            value_string = ", ".join(values_insert)
            self._cursor.execute(self._insert_query.format(
                table_name, column_string, value_string))
            self._pg_db.commit()
            records = self._cursor.fetchall()
            print("Output: ", records)
        else:
            print("The length of columns and values don't match at least for one element")

    def from_csv_to_psql(self, filename, table_name, column_list=None, sep=','):
        """Saves csv records to postgresql

        Args:
            filename (string): name of the file in the current location
            table_name (string): name of the table
            column_list (tuple): tuple of the names of the columns (string). The length and types should match the content of the file to read. If not specified, it is assumed that the entire table matches the file structure.
            sep (str, optional): csv seperator. Defaults to ','.
        """
        with open(filename) as f:
            self._cursor.copy_from(f, table_name, sep, columns=column_list)
        self._pg_db.commit()

    def from_excel_to_psql(self, filename, sheet_number, table_name, column_list, headers=False):
        """Import excel to the database. The number of columns in Excel must match with passed column list.

        Args:
            filename (string): name of the file in the current location
            sheet_number (int): the number of the sheet. It begins from 0
            table_name (string): name of the table
            column_list (tuple or list): tuple of the names of the columns (string). The length and types should match the content of the file to read. If not specified, it is assumed that the entire table matches the file structure.
            headers (bool, optional): if headers are in Excel then True. Defaults to False.
        """
        book = xlrd.open_workbook(filename=filename)
        sheet = book.sheet_by_index(sheet_number)
        num_columns = sheet.ncols
        if num_columns == len(column_list):
            num_rows = sheet.nrows
            rows_range = range(0 if headers == False else 1, num_rows)
            for row_list in rows_range:
                values = sheet.row_values(row_list)
                for i, cell in enumerate(values):
                    if isinstance(cell, str):
                        values[i] = "'" + cell + "'"
                self.save_match_to_psql(table_name, column_list, values)
        else:
            print("The length of columns and values don't match at least for one element")

    def close(self):
        """Close cursor and database
        """
        self._cursor.close()
        self._pg_db.close()
