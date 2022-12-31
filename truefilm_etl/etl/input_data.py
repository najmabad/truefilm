import os

from pyspark.sql import DataFrame

import subprocess

import pyspark.sql.functions as f
from pyspark.sql import types as t

from config import (
    BUDGET,
    CLEANED_TITLE,
    GENRES,
    PRODUCTION_COMPANIES,
    RELEASE_DATE,
    REVENUE,
    REVENUE_TO_BUDGET,
    TITLE,
    TYPE,
    URL,
    WIKI_YEAR,
    YEAR,
)

from truefilm_etl.utils.spark import SparkFileDAO, SparkDataFrame

import logging.config

logging.config.fileConfig(fname="logging.config", disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)


class WikipediaData(SparkFileDAO, SparkDataFrame):
    """A class for handling and processing Wikipedia data stored in an XML file.

    The class inherits from `SparkFileDAO` and `SparkDataFrame` to provide methods for reading and writing data to and
    from files, and for manipulating and transforming data stored in Apache Spark DataFrames.

    Args:
        wikipedia_file_path (str): The file path of the Wikipedia abstracts XML file.
    """

    def __init__(self, wikipedia_file_path: str):
        super().__init__()
        self.wiki_abstracts_filepath = wikipedia_file_path
        self.sample_wiki_filepath = "sample-wiki.xml"

    def _get_dataframe_schema(self, lines: str = "100"):
        """Retrieves the schema for the XML data in the Wikipedia abstracts file by parsing a given number of lines.

        Args:
            lines: The number of lines to read from the beginning of the file.

        Returns:
            The schema of the XML data.
        """
        logger.info(
            f"Retrieving schema from first {lines} of {self.wiki_abstracts_filepath}"
        )

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
        logger.info("Reading raw Wikipedia data")
        return self.read_xml(
            rowtag="doc",
            filepath=self.wiki_abstracts_filepath,
            schema=self._get_dataframe_schema(),
        )

    @staticmethod
    def add_cleaned_title_type_and_year_columns(
        df: DataFrame, url_column: str
    ) -> DataFrame:
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
        logger.info("Creating cleaned_title, year, and type column")
        # Define the regex pattern to match the year, object_type, and cleaned_title in the URL
        pattern = r"([^\/]*)\((\d+)?_?(\w+)?\)"

        # Extract the cleaned title, year, and type from the URL
        df = df.withColumn(CLEANED_TITLE, f.regexp_extract(url_column, pattern, 1))
        df = df.withColumn(WIKI_YEAR, f.regexp_extract(url_column, pattern, 2))
        df = df.withColumn(TYPE, f.regexp_extract(url_column, pattern, 3))

        # If the year and type columns are empty, set the cleaned_title to everything after the last '/' in the URL
        df = df.withColumn(
            CLEANED_TITLE,
            f.when(
                (f.col(WIKI_YEAR) == "") & (f.col(TYPE) == ""),
                f.element_at(f.split(URL, "/"), -1),
            ).otherwise(f.col(CLEANED_TITLE)),
        )

        # Clean up the cleaned_title column by replacing underscores with spaces and lower-casing the text
        df = df.withColumn(
            CLEANED_TITLE, f.lower(f.trim(f.regexp_replace(CLEANED_TITLE, "_", " ")))
        )

        return df


class IMDBData(SparkFileDAO, SparkDataFrame):
    """A class for handling and processing IMDB data stored in a CSV file.

    The class inherits from `SparkFileDAO` and `SparkDataFrame` to provide methods for reading and writing data to and
    from files, and for manipulating and transforming data stored in Apache Spark DataFrames.

    Args:
        imdb_file_path (str): The file path of the IMDB CSV file.
    """

    def __init__(self, imdb_file_path: str):
        super().__init__()
        self.imdb_movies_metadata = imdb_file_path

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

    def raw_data(self) -> DataFrame:
        logger.info("Reading raw IMDB data")
        return self.read_csv(filepath=self.imdb_movies_metadata)

    def cast_columns(self, df: DataFrame) -> DataFrame:
        logger.info("Casting columns to correct types")
        for column, column_type in self.column_types.items():
            if column_type == self.json_column_schema:
                df = df.withColumn(column, f.from_json(column, self.json_column_schema))
            else:
                df = df.withColumn(column, f.col(column).cast(column_type))
        return df

    def transform_json_columns_to_name_only(self, df: DataFrame) -> DataFrame:
        logger.info(
            "Transforming JSON columns and keep only the name (i.e. drop the id)"
        )
        for column in self.column_types.keys():
            if self.column_types[column] == self.json_column_schema:
                df = df.withColumn(column, f.col(f"{column}.name"))
        return df

    @staticmethod
    def extract_year_from_release_date(df: DataFrame) -> DataFrame:
        logger.info("Extracting year from release_date")
        df = df.withColumn(YEAR, f.year(f.col(RELEASE_DATE)))
        df = df.drop(RELEASE_DATE)
        return df

    @staticmethod
    def add_revenue_to_budget_column(df: DataFrame) -> DataFrame:
        logger.info("Adding revenue to budget column")
        df = df.withColumn(
            REVENUE_TO_BUDGET, f.round(f.col(REVENUE) / f.col(BUDGET), 2)
        )
        return df

    @staticmethod
    def add_cleaned_title_column(df: DataFrame) -> DataFrame:
        logger.info("Adding cleaned_title column")
        cleaned_title_column = f.lower(f.trim(df[TITLE]))
        return df.withColumn(CLEANED_TITLE, cleaned_title_column)
