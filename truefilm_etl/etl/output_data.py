import psycopg2

import os


class PostgresDAO:
    """Data access object for interacting with a PostgreSQL database.

    This class provides methods for writing data to a PostgreSQL table and
    reading data from a PostgreSQL table.
    """

    def __init__(self):
        """Initializes a connection to the PostgreSQL database.

        The connection parameters are read from environment variables. The
        following environment variables are expected:
         - POSTGRES_HOST: The hostname or IP address of the PostgreSQL server.
         - POSTGRES_PORT: The port number on which the PostgreSQL server is listening.
         - POSTGRES_USER: The username to use for connecting to the PostgreSQL server.
         - POSTGRES_PASSWORD: The password for the user.
         - POSTGRES_DATABASE: The name of the database to connect to.
        """
        host = os.environ["POSTGRES_HOST"]
        port = os.environ["POSTGRES_PORT"]
        user = os.environ["POSTGRES_USER"]
        password = os.environ["POSTGRES_PASSWORD"]
        database = os.environ["POSTGRES_DATABASE"]

        self.conn = psycopg2.connect(
            host=host, port=port, user=user, password=password, database=database
        )

    def write_to_table(self, df, table_name, mode="overwrite"):
        """Writes a DataFrame to a PostgreSQL table.

        Args:
            df: The DataFrame to write.
            table_name: The name of the table to write to.
            mode: The write mode for the DataFrame. Can be 'overwrite' to overwrite
                the existing table, or 'append' to append the data to the existing
                table. Defaults to 'overwrite'.
        """
        df.write.jdbc(
            url=self.conn.get_dsn_parameters()["host"],
            table=table_name,
            mode=mode,
            properties=dict(user=self.conn.info.user, password=self.conn.info.password),
        )
