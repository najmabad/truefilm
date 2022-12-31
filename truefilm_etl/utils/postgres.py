import os

from config import REVENUE_TO_BUDGET
from truefilm_etl.utils.spark import MySparkSession

import logging.config

logging.config.fileConfig(fname="logging.config", disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)


class PostgresDAO(MySparkSession):
    """Data access object for interacting with a PostgreSQL database.

    This class provides methods for writing data to a PostgreSQL table and
    reading data from a PostgreSQL table.
    """

    def __init__(self):
        super().__init__()
        self.postgres_host = os.environ["POSTGRES_HOST"]
        self.postgres_port = os.environ["POSTGRES_PORT"]
        self.postgres_database = os.environ["POSTGRES_DATABASE"]
        self.postgres_user = os.environ["POSTGRES_USER"]
        self.postgres_password = os.environ["POSTGRES_PASSWORD"]
        self.url = f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_database}"
        self.properties = {
            "user": self.postgres_user,
            "password": self.postgres_password,
            "driver": "org.postgresql.Driver",
        }

    def write_to_table(
        self,
        df,
        table_name,
        mode="overwrite",
        order_by_col: str = None,
        rows: int = None,
    ):
        """Writes a DataFrame to a PostgreSQL table.

        Args:
            df: The DataFrame to write.
            table_name: The name of the table to write to.
            mode: The write mode for the DataFrame. Can be 'overwrite' to overwrite
                the existing table, or 'append' to append the data to the existing
                table. Defaults to 'overwrite'.
        """
        logger.info(f"Writing to table '{table_name}'")

        if order_by_col:
            logger.info("Sorting the data by column: " + order_by_col)
            df = df.orderBy(REVENUE_TO_BUDGET, ascending=False)

        if rows:
            logger.info("Limiting to " + str(rows) + " rows")
            df = df.limit(rows)

        df.write.jdbc(
            url=self.url, table=table_name, mode=mode, properties=self.properties
        )

    def read_from_table(self, table_name):
        """Reads data from a PostgreSQL table.

        Args:
            table_name: The name of the table to read from.

        Returns:
            A DataFrame containing the data from the table.
        """

        df = self.spark.read.jdbc(
            url=self.url, table=table_name, properties=self.properties
        )
        return df
