# import pytest
# from pyspark.sql import DataFrame, SparkSession
#
# from truefilm_etl.etl.input_data import SparkFileDAO, SparkDataFrame
#
#
#
# @pytest.fixture
# def spark_file_dao(spark):
#     """Fixture that returns an instance of the FileDAO class."""
#     return SparkFileDAO()
#
#
#
#
# # Create a sample dataframe as a fixture
# @pytest.fixture
# def df(spark):
#     df = spark.createDataFrame(
#         [("Amelie", 2001), ("A Clockwork Orange ", 1971), ("Mozart and the Whale", 2005)],
#         ["title", "year"]
#     )
#     return df
#
#
# def test_spark_session(spark):
#     # Assert
#     assert isinstance(spark, SparkSession)
#
#
# def test_read_xml(spark_file_dao, schema):
#     # Arrange
#     rowtag = "doc"
#     filepath = "tests/data/enwiki-latest-abstract-test.xml"
#     schema = schema
#
#     # Act
#     df = spark_file_dao.read_xml(rowtag=rowtag, filepath=filepath, schema=schema)
#
#     # Assert
#     assert isinstance(df, DataFrame)
#
#
# def test_read_csv(spark_file_dao):
#     # Arrange
#     filepath = "tests/data/movies-metadata-test.csv"
#     header = True
#     quote = '"'
#     escape = "'"
#
#     # Act
#     df = spark_file_dao.read_csv(filepath=filepath, header=header, quote=quote, escape=escape)
#
#     # Assert
#     assert isinstance(df, DataFrame)
#
#
# def test_repartitioned_df(df):
#     # Arrange
#     partition_column = "title"
#
#     # Act
#     repartitioned_df = SparkDataFrame.repartitioned_df(df, partition_column)
#
#     # Assert
#     assert repartitioned_df.rdd.getNumPartitions() == 1
#
#
# def test_subset_df(df):
#     # Test sub-setting the dataframe to keep only the 'title' column
#     subset_df = SparkDataFrame.subset_df(df, ["title"])
#     assert set(subset_df.columns) == {"title"}
#
#
# def test_rename_columns(df):
#     # Test renaming the 'title' column to 'cleaned_title'
#     renamed_df = SparkDataFrame.rename_columns(df, {"title": "cleaned_title"})
#     assert "title" not in renamed_df.columns
#     assert "cleaned_title" in renamed_df.columns
#
#
# def test_filtered_df(df):
#     # Create a filter condition for movies released after 2000
#     filter_conditions = {"year > 2000": df["year"] > 2000}
#
#     # Filter the dataframe using the filter condition
#     filtered_df = SparkDataFrame.filtered_df(df, filter_conditions)
#
#     # Assert that the filtered dataframe only contains movies released after 2000
#     assert all(filtered_df.select("year").toPandas()["year"] > 2000)
#
