import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import DataFrame

from config import (
    ABSTRACT,
    BUDGET,
    CLEANED_TITLE,
    GENRES,
    PRODUCTION_COMPANIES,
    RATING,
    RELEASE_DATE,
    REVENUE,
    REVENUE_TO_BUDGET,
    TITLE,
    TYPE,
    URL,
    VOTE_AVERAGE,
    WIKI_YEAR,
    YEAR,
)
from truefilm_etl.etl.input_data import IMDBData, WikipediaData

import logging.config

logging.config.fileConfig(fname="logging.config", disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)


class WikipediaProcessor:
    """Processes raw Wikipedia data and prepares it for joining with IMDB data.

    This class processes the raw data from the Wikipedia abstracts file to prepare it for joining with IMDB data. It
    extracts the cleaned_title, year, and object_type from the url_column using a regex pattern and adds these
    extracted values as new columns to the dataframe. It also removes the 'title' and 'links' columns from the dataframe.
    """

    def __init__(self, wikipedia_file_path: str):
        self.wikipedia_data = WikipediaData(wikipedia_file_path)

    def df(self):
        """Return a dataframe with all the pre-processing necessary to join the dataset with the IMDB data"""
        logger.info(f"\n {'*' * 20} Processing Wikipedia data {'*' * 20}")
        raw_data = self.wikipedia_data.raw_data()
        df = self.wikipedia_data.add_cleaned_title_type_and_year_columns(raw_data, URL)
        df = self.wikipedia_data.repartitioned_df(
            df=df, partition_column="cleaned_title"
        )
        df = df.drop("title", "links")
        return df


class IMDBProcessor:
    def __init__(self, imdb_file_path: str):
        """
        The IMDBProcessor class processes the raw IMDB data and returns a dataframe that is ready
        to be joined with the Wikipedia data in the ETL pipeline. The class takes an IMDB file path as input and
        initializes an IMDBData object with that file path. It then defines the key columns to keep,
        the column names to rename, and the filter conditions to apply to the data.

        The df() method applies these processing steps to the raw data, including sub-setting the data to only
        include the specified key columns, renaming the columns, casting the columns to the correct data types,
        applying filters to the data, transforming JSON columns to a list of names, extracting the year from
        the release date, adding a column for the ratio of revenue to budget, adding a column for the cleaned title,
        and repartitioning the data by the cleaned title column.

        The method returns a dataframe with the columns title, year, production_companies, genres, rating,
        revenue, budget, revenue_to_budget, and cleaned_title.
        """
        self.imdb_data = IMDBData(imdb_file_path=imdb_file_path)
        self.key_columns = [
            TITLE,
            GENRES,
            PRODUCTION_COMPANIES,
            RELEASE_DATE,
            VOTE_AVERAGE,
            BUDGET,
            REVENUE,
        ]

        self.column_names = {VOTE_AVERAGE: RATING}

        self.filter_conditions = {
            "title_not_null": f.col(TITLE) != "",
            # "genres_not_null": f.col(GENRES).isNotNull(), # maybe include this with a note in the README.md
            # "production_companies_not_null": f.col(PRODUCTION_COMPANIES).isNotNull(),
            "revenue_gt_zero": f.col(REVENUE) > 0,
            "budget_gt_zero": f.col(BUDGET) > 0,
        }

    def df(self):
        """Return a dataframe with all the pre-processing necessary to join the dataset with the Wikipedia data"""
        logger.info(f"\n {'*' * 20} Processing IMDB data {'*' * 20}")
        raw_data = self.imdb_data.raw_data()
        df = self.imdb_data.subset_df(df=raw_data, key_columns=self.key_columns)
        df = self.imdb_data.rename_columns(df=df, column_names=self.column_names)
        df = self.imdb_data.cast_columns(df=df)
        df = self.imdb_data.filtered_df(df=df, filter_conditions=self.filter_conditions)
        df = self.imdb_data.transform_json_columns_to_name_only(df=df)
        df = self.imdb_data.extract_year_from_release_date(df=df)
        df = self.imdb_data.add_revenue_to_budget_column(df=df)
        df = self.imdb_data.add_cleaned_title_column(df=df)
        df = self.imdb_data.repartitioned_df(df=df, partition_column=CLEANED_TITLE)
        df = df.withColumn(YEAR, f.col(YEAR).cast(t.StringType()))

        return df.select(
            TITLE,
            YEAR,
            PRODUCTION_COMPANIES,
            GENRES,
            RATING,
            REVENUE,
            BUDGET,
            REVENUE_TO_BUDGET,
            CLEANED_TITLE,
        )


class MatchData:
    """
    MatchData is a class that performs matching between two DataFrames: one containing data from IMDB, and the other
    containing data from Wikipedia.

    The matching is done in several steps:

    Match the IMDB data on title, year, and type with the Wikipedia data where the year and type are both not empty.
    Exclude the IMDB data that was matched in step 1 from the pool of 'matchable' data.
    Match the remaining 'matchable' IMDB data on title and type with the Wikipedia data where the year is empty
    and the type is not empty.
    Exclude the IMDB data that was matched in step 3 from the pool of 'matchable' data.
    Match the remaining 'matchable' IMDB data on title with the Wikipedia data where the year and type are both empty.
    Select only the specified columns from the three matched DataFrames.
    Union the three matched DataFrames.
    Attributes:

    imdb_df (pyspark.sql.dataframe.DataFrame): DataFrame containing data from IMDB.
    wiki_df (pyspark.sql.dataframe.DataFrame): DataFrame containing data from Wikipedia.
    Methods:

    match (): Performs the matching between the IMDB and Wikipedia data and returns a new DataFrame containing the
    matched data.
    """

    def __init__(self, imdb_df: DataFrame, wiki_df: DataFrame):
        self.imdb_df = imdb_df
        self.wiki_df = wiki_df

    def match(self):
        logger.info(
            "Step 1: Keep only the Wikipedia rows where wiki_year is not empty and "
            "type == 'film' in order to match the IMDB data on title, year, and type."
        )
        wiki_year_and_type = self.wiki_df.where(
            (f.col(WIKI_YEAR) != "") & (f.col(TYPE) == "film")
        )

        df_matched_year_and_type = self.imdb_df.join(
            wiki_year_and_type, on=[CLEANED_TITLE, YEAR], how="inner"
        )

        logger.info("Step 2: Exclude the IMDB matched rows from the 'matchable' pool")
        matchable_imdb_df = self.imdb_df.subtract(
            df_matched_year_and_type.select(self.imdb_df.columns)
        )

        logger.info(
            "Step 3: Keep only the Wikipedia rows where wiki_year is empty and type == 'film'"
            " in order to match the IMDB data on title and type"
        )
        wiki_type = self.wiki_df.where(
            (f.col(WIKI_YEAR) == "") & (f.col(TYPE) == "film")
        ).drop(WIKI_YEAR)

        df_matched_type = matchable_imdb_df.join(
            wiki_type, on=[CLEANED_TITLE], how="inner"
        )

        logger.info("Step 4: Exclude the IMDB matched rows from the 'matchable' pool")
        matchable_imdb_df = matchable_imdb_df.subtract(
            df_matched_type.select(self.imdb_df.columns)
        )

        logger.info(
            "Step 5: Keep only the Wikipedia rows where wiki_year is empty and type == ''"
            "  in order to match the IMDB data on title"
        )
        wiki_empty_type = self.wiki_df.where(
            (f.col(WIKI_YEAR) == "") & (f.col(TYPE) == "")
        ).drop(WIKI_YEAR)

        df_matched_title = matchable_imdb_df.join(
            wiki_empty_type, on=[CLEANED_TITLE], how="left"
        )

        logger.info("Step 6: Define the columns to keep")
        columns = [
            TITLE,
            YEAR,
            PRODUCTION_COMPANIES,
            GENRES,
            RATING,
            REVENUE,
            BUDGET,
            REVENUE_TO_BUDGET,
            URL,
            ABSTRACT,
        ]

        logger.info(
            "Step 7: Union the three matched dataframes, and select only the specified columns"
        )
        df_matched = (
            df_matched_year_and_type.select(columns)
            .union(df_matched_type.select(columns))
            .union(df_matched_title.select(columns))
        )

        return df_matched
