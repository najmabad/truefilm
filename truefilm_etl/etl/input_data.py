from pyspark.sql import SparkSession, DataFrame
import os
import logging

# Set the logging level
logging.basicConfig(level=logging.INFO)

from truefilm_etl.config import (
    INPUT_DATA_DIR,
    WIKI_ABSTRACTS_FILEPATH,
    IMBD_MOVIES_METADATA_FILEPATH,
    TITLE,
    GENRES,
    PRODUCTION_COMPANIES,
    RELEASE_DATE,
    VOTE_AVERAGE,
    BUDGET,
    REVENUE,
    RATING,
    YEAR,
    REVENUE_TO_BUDGET, CLEANED_TITLE,
)

import pyspark.sql.functions as f

from pyspark.sql import types as t
import subprocess


class FileDAO:
    def __init__(self):
        self.spark = (
            SparkSession.builder.appName("Data Access Object")
            .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.15.0")
            .getOrCreate()
        )

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
    def __init__(self, df: DataFrame):
        self.df = df

    @staticmethod
    def repartition_df(df: DataFrame, partition_column: str) -> DataFrame:
        """Repartition the DataFrame using a given column.

        Args:
            df: The DataFrame to be repartitioned.
            partition_column: The column to use to repartition the DataFrame.

        Returns:
            The repartitioned DataFrame.
        """
        logging.info(f"Repartitioning the dataframe using the '{partition_column}' column")
        return df.repartition(partition_column)

    #
    # def get_df(self) -> DataFrame:
    #     return self.df
    #
    # def get_df_schema(self) -> t.StructType:
    #     return self.df.schema
    #
    # def get_df_columns(self) -> list:
    #     return self.df.columns
    #
    # def get_df_count(self) -> int:
    #     return self.df.count()
    #
    # def get_df_distinct_count(self, column: str) -> int:
    #     return self.df.select(column).distinct().count()
    #
    # def get_df_sample(self, n: int = 5) -> DataFrame:
    #     return self.df.sample(False, 0.01, seed=42).limit(n)
    #
    # def get_df_describe(self) -> DataFrame:
    #     return self.df.describe()
    #
    # def get_df_describe_column(self, column: str) -> DataFrame:
    #     return self.df.describe(column)
    #
    # def get_df_column_stats(self, column: str) -> DataFrame:
    #     return self.df.select(column).summary()
    #
    # def get_df_column_stats_by_group(self, column: str, group_by: str) -> DataFrame:
    #     return self.df.groupby(group_by).agg(f.count(column


class WikipediaData(FileDAO, SparkDataFrame):
    def __init__(self):
        super().__init__()
        self.input_data = INPUT_DATA_DIR
        self.wiki_abstracts_filepath = WIKI_ABSTRACTS_FILEPATH
        self.sample_wiki_filepath = os.path.join(INPUT_DATA_DIR, "sample-wiki.xml")

    def _get_dataframe_schema(self, lines: str = "100"):
        """Retrieves the schema for the XML data in the Wikipedia abstracts file by parsing a given number of lines.

        Args:
            lines: The number of lines to read from the beginning of the file.

        Returns:
            The schema of the XML data.
        """

        # Run the "head" command on the filepath and capture the output as a byte string
        output = subprocess.run(
            ["head", "-n", lines, self.wiki_abstracts_filepath], capture_output=True
        ).stdout

        # Decode the output byte string as UTF-8 and store it as a string
        output_string = output.decode("utf-8")

        # Find the index of the last occurrence of </doc>
        last_doc_index = output_string.rindex("</doc>")

        # Remove everything after the last </doc>
        trimmed_string = output_string[: last_doc_index + len("</doc>")]

        # Add the </feed> string to the end of the trimmed string
        final_string = trimmed_string + "</feed>"

        # Create a sample XML file from the final string
        with open(self.sample_wiki_filepath, "w") as outfile:
            outfile.write(final_string)

        # Read the sample file
        df_sample = self.read_xml(rowtag="doc", filepath=self.sample_wiki_filepath)

        # Retrieve the schema
        schema = df_sample.schema

        # Delete the sample file
        subprocess.run(["rm", self.sample_wiki_filepath])

        return schema

    def raw_data(self) -> DataFrame:
        """Retrieves the raw data from the Wikipedia abstracts file.

        Returns:
            The raw data as a DataFrame.
        """
        return self.read_xml(
            rowtag="doc",
            filepath=self.wiki_abstracts_filepath,
            schema=self._get_dataframe_schema(),
        )

    @staticmethod
    def add_cleaned_title_type_and_year_columns(df: DataFrame, url_column: str) -> DataFrame:
        """Add cleaned_title, object_type, and year columns to the DataFrame.

        This method extracts the cleaned_title, year, and object_type from the url_column in the input DataFrame using
        a regex pattern. The pattern looks for a string that starts with one or more characters that are not a forward
        slash ([^\/]*), followed by an open parenthesis, then a series of digits (the year) that are optionally
        followed by an underscore and a series of word characters (the type), and finally a close parenthesis.
        If the year and object_type cannot be extracted from the url_column using this pattern, the cleaned_title is
        set to everything after the last '/' in the URL.
        The cleaned_title column is cleaned up by replacing underscores with spaces, lower-casing the text, and
        trimming any white spce.

        Parameters:
        - df (pyspark.sql.dataframe.DataFrame): DataFrame to which the new columns will be added.
        - url_column (str): Name of the column in df that contains the URLs from which the cleaned_title, year, and
                           object_type will be extracted.

        Returns:
        - pyspark.sql.dataframe.DataFrame: A new DataFrame with the added columns cleaned_title, year, and object_type.
        """
        # Define the regex pattern to match the year, object_type, and cleaned_title in the URL
        pattern = r"([^\/]*)\((\d+)?_?(\w+)?\)"

        # Extract the cleaned title, year, and type from the URL
        df = df.withColumn("cleaned_title", f.regexp_extract(url_column, pattern, 1))
        df = df.withColumn("year", f.regexp_extract(url_column, pattern, 2))
        df = df.withColumn("type", f.regexp_extract(url_column, pattern, 3))

        # If the year and type columns are empty, set the cleaned_title to everything after the last '/' in the URL
        df = df.withColumn(
            "cleaned_title",
            f.when(
                (f.col("year") == '') & (f.col("type") == ''),
                f.element_at(f.split("url", "/"), -1),
            ).otherwise(f.col("cleaned_title")),
        )

        # Clean up the cleaned_title column by replacing underscores with spaces and lower-casing the text
        df = df.withColumn(
            "cleaned_title", f.lower(f.trim(f.regexp_replace("cleaned_title", "_", " ")))
        )

        return df


class IMDBData(FileDAO, SparkDataFrame):
    def __init__(self):
        super().__init__()
        self.imdb_movies_metadata = IMBD_MOVIES_METADATA_FILEPATH
        self.key_columns = [
            TITLE,
            GENRES,
            PRODUCTION_COMPANIES,
            RELEASE_DATE,
            VOTE_AVERAGE,
            BUDGET,
            REVENUE,
        ]

        self.json_column_schema = t.ArrayType(
            t.StructType(
                [
                    t.StructField("id", t.IntegerType()),
                    t.StructField("name", t.StringType()),
                ]
            )
        )

        self.column_types = {
            BUDGET: "long",
            RELEASE_DATE: "date",
            REVENUE: "long",
            PRODUCTION_COMPANIES: self.json_column_schema,
            GENRES: self.json_column_schema,
        }

        self.column_names = {VOTE_AVERAGE: RATING}

        self.filter_conditions = {
            "title_not_null": f.col(TITLE).isNotNull(),
            # "genres_not_null": f.col(GENRES).isNotNull(), # maybe include this with a note in the README.md
            # "production_companies_not_null": f.col(PRODUCTION_COMPANIES).isNotNull(),
            "revenue_gt_zero": f.col(REVENUE) > 0,
            "budget_gt_zero": f.col(BUDGET) > 0,
        }

    def raw_data(self) -> DataFrame:
        return self.read_csv(filepath=self.imdb_movies_metadata)

    def raw_data_column_subset(self, df: DataFrame) -> DataFrame:
        return df.select(self.key_columns)

    def rename_columns(self, df: DataFrame) -> DataFrame:
        for column, column_name in self.column_names.items():
            df = df.withColumnRenamed(column, column_name)
        return df

    def cast_columns(self, df: DataFrame) -> DataFrame:
        for column, column_type in self.column_types.items():
            if column_type == self.json_column_schema:
                df = df.withColumn(column, f.from_json(column, self.json_column_schema))
            else:
                df = df.withColumn(column, f.col(column).cast(column_type))
        return df

    def filtered_data(self, df):
        for condition, value in self.filter_conditions.items():
            df = df.filter(value)
        return df

    def transform_json_columns_to_name_only(self, df: DataFrame) -> DataFrame:
        for column in self.column_types.keys():
            if self.column_types[column] == self.json_column_schema:
                df = df.withColumn(column, f.col(f"{column}.name"))
        return df

    @staticmethod
    def extract_year_from_release_date(df: DataFrame) -> DataFrame:
        df = df.withColumn(YEAR, f.year(f.col(RELEASE_DATE)))
        df = df.drop(RELEASE_DATE)
        return df

    @staticmethod
    def add_revenue_to_budget_column(df: DataFrame) -> DataFrame:
        df = df.withColumn(
            REVENUE_TO_BUDGET, f.round(f.col(REVENUE) / f.col(BUDGET), 2)
        )
        return df

    @staticmethod
    def add_cleaned_title_column(df: DataFrame) -> DataFrame:
        cleaned_title_column = f.lower(f.trim(df[TITLE]))
        return df.withColumn(CLEANED_TITLE, cleaned_title_column)
