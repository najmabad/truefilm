from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as t

from config import JARS_FILE_PATHS

import logging.config

logging.config.fileConfig(fname="logging.config", disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)


class MySparkSession:
    def __init__(self):
        self.spark = (
            SparkSession.builder.appName("Data Access Object")
            .config("spark.jars", JARS_FILE_PATHS)
            .getOrCreate()
        )


class SparkFileDAO(MySparkSession):
    def __init__(self):
        super().__init__()

    def read_xml(
        self, rowtag: str, filepath: str, schema: t.StructType = None
    ) -> DataFrame:
        return (
            self.spark.read.format("xml")
            .option("rowtag", rowtag)
            .load(path=filepath, schema=schema)
        )

    def read_csv(
        self, filepath: str, header: bool = True, quote: str = '"', escape: str = "'"
    ) -> DataFrame:
        return self.spark.read.csv(
            path=filepath, header=header, quote=quote, escape=escape
        )


class SparkDataFrame:
    @staticmethod
    def repartitioned_df(df: DataFrame, partition_column: str) -> DataFrame:
        """Repartition the DataFrame using a given column.

        Args:
            df: The DataFrame to be repartitioned.
            partition_column: The column to use to repartition the DataFrame.

        Returns:
            The repartitioned DataFrame.
        """
        logging.info(
            f"Repartitioning the dataframe using the '{partition_column}' column"
        )
        return df.repartition(partition_column)

    @staticmethod
    def subset_df(df: DataFrame, key_columns: list) -> DataFrame:
        return df.select(key_columns)

    @staticmethod
    def rename_columns(df: DataFrame, column_names: dict) -> DataFrame:
        for column, column_name in column_names.items():
            df = df.withColumnRenamed(column, column_name)
        return df

    @staticmethod
    def filtered_df(df: DataFrame, filter_conditions: dict) -> DataFrame:
        for condition, value in filter_conditions.items():
            df = df.filter(value)
        return df

    #
    # def get_df_describe_column(self, column: str) -> DataFrame:
    #     return self.df.describe(column)
    #
    # def get_df_column_stats(self, column: str) -> DataFrame:
    #     return self.df.select(column).summary()
