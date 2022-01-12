import pyodbc
import logging


# TODO write a test class

class DatabaseConnectionTool:
    def __init__(self, connection_string=None):
        self._connection = self._create_connection(connection_string)
        self._log = logging.getLogger("DatabaseConnectionTool")

    def create(self, query_string):
        return self._execute(query_string=query_string)

    def update(self, query_string):
        return self._execute(query_string=query_string)

    def read(self, query_string):
        return self._execute(query_string=query_string)

    def delete(self, query_string):
        return self._execute(query_string=query_string)

    def close(self):
        if self._connection is not None:
            self._connection.close()
            self._log.info('Connection Closed')
        else:
            self._log.info("No Connection to close")

    def _execute(self, query_string):
        if self._connection is None:
            raise ValueError("Connection String has not been set")
        try:
            cursor: pyodbc.Cursor
            cursor = self._connection.cursor()
            cursor.execute(query_string)
            for value in cursor.fetchall():
                yield value
            cursor.commit()
            cursor.close()
        except pyodbc.Error as err:
            self._log.error(err)

    def _create_connection(self, connection_string):
        try:
            connection = pyodbc.connect(
                p_str=connection_string
            )
            if self._connection is None:
                return connection
            else:
                raise ValueError("Connection is already created please close the connection")
        except ConnectionError as err:
            self._log.error(err)


def drivers():
    """
    Helper function, Checks and returns for the newest SQL database driver.
    """
    found_drivers = pyodbc.drivers()
    return found_drivers[0] if len(found_drivers) >= 1 else "No Driver Found"
