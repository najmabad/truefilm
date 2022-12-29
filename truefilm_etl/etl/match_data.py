import pyspark.sql.functions as f

from truefilm_etl.config import URL
from truefilm_etl.etl.input_data import WikipediaData, IMDBData
from pyspark.sql import DataFrame

from truefilm_etl.config import (
    TITLE,
    GENRES,
    PRODUCTION_COMPANIES,
    BUDGET,
    REVENUE,
    RATING,
    YEAR,
    REVENUE_TO_BUDGET, CLEANED_TITLE,
)


class WikipediaProcessor:
    def __init__(self):
        self.wikipedia_data = WikipediaData()

    def df(self):
        """Return a dataframe with all the pre-processing necessary to join the dataset with the IMDB data"""
        raw_data = self.wikipedia_data.raw_data()
        df = self.wikipedia_data.add_cleaned_title_type_and_year_columns(raw_data, URL)
        df = self.wikipedia_data.repartition_df(df=df, partition_column="cleaned_title")
        df = df.drop("title", "links")
        df = df.withColumnRenamed("year", "wikipedia_year")
        return df


class IMDBProcessor:
    def __init__(self):
        self.imdb_data = IMDBData()

    def df(self):
        """Return a dataframe with all the pre-processing necessary to join the dataset with the Wikipedia data"""
        raw_data = self.imdb_data.raw_data()
        df = self.imdb_data.raw_data_column_subset(raw_data)
        df = self.imdb_data.rename_columns(df)
        df = self.imdb_data.cast_columns(df)
        df = self.imdb_data.filtered_data(df)
        df = self.imdb_data.transform_json_columns_to_name_only(df)
        df = self.imdb_data.extract_year_from_release_date(df)
        df = self.imdb_data.add_revenue_to_budget_column(df)
        df = self.imdb_data.add_cleaned_title_column(df)
        df = self.imdb_data.repartition_df(df=df, partition_column="cleaned_title")

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
    def __init__(self, left_df: DataFrame, right_df: DataFrame, join_column: str):
        self.left_df = left_df
        self.right_df = right_df
        self.join_column = join_column

    def join(self, method: str = None):
        if method == 'broadcast':
            df = self.right_df.join(f.broadcast(self.left_df), on=self.join_column, how='right')

        else:
            df = self.left_df.join(self.right_df, on=self.join_column, how='left')

        return df


WikipediaProcessor().df().show()
IMDBProcessor().df().show()
