import pytest

from config import CLEANED_TITLE, WIKI_YEAR, TYPE, TITLE, URL
from truefilm_etl.etl.input_data import WikipediaData


@pytest.fixture
def wikipedia_data():
    """Fixture that returns an instance of the WikipediaData class using sample data."""
    wd = WikipediaData(wikipedia_file_path="tests/data/enwiki-latest-abstract-test.xml")
    return wd


@pytest.fixture
def wiki_df(spark):
    df = spark.createDataFrame(
        [
            ("Wikipedia: Vollmer House", "https://en.wikipedia.org/wiki/Vollmer_House"),
            (
                "Wikipedia: Antwone Fisher",
                "https://en.wikipedia.org/wiki/Antwone_Fisher",
            ),
            (
                "Wikipedia: Antwone Fisher (film)",
                "https://en.wikipedia.org/wiki/Antwone_Fisher_(film)",
            ),
            (
                "Wikipedia: King Kong Groover",
                "https://en.wikipedia.org/wiki/King_Kong_Groover",
            ),
            (
                "Wikipedia: King Kong (comics)",
                "https://en.wikipedia.org/wiki/King_Kong_(comics)",
            ),
            (
                "Wikipedia: King Kong (1959 musical)",
                "https://en.wikipedia.org/wiki/King_Kong_(1959_musical)",
            ),
            (
                "Wikipedia: King Kong (1933 film)",
                "https://en.wikipedia.org/wiki/King_Kong_(1933_film)",
            ),
            (
                "Wikipedia: King Kong (1976 film)",
                "https://en.wikipedia.org/wiki/King_Kong_(1976_film)",
            ),
        ],
        ["title", "url"],
    )
    return df


# def test__get_dataframe_schema(wikipedia_data, schema):
#     # Call the _get_dataframe_schema method with default arguments
#     result = wikipedia_data._get_dataframe_schema()
#
#     # Assert that the result is equal to the schema fixture
#     assert result == schema
#
#
# def test_raw_data(wikipedia_data, wiki_df):
#     # Call the raw_data method with default arguments
#     result = wikipedia_data.raw_data()
#
#     # Assert that the result is equal to the wiki_df fixture
#     assert result.select("title", "url").collect() == wiki_df.collect()


def test_add_cleaned_title_type_and_year_columns(spark, wiki_df):
    # Arrange
    expected_df = spark.createDataFrame(
        [
            (
                "Wikipedia: Vollmer House",
                "https://en.wikipedia.org/wiki/Vollmer_House",
                "vollmer house",
                "",
                "",
            ),
            (
                "Wikipedia: Antwone Fisher",
                "https://en.wikipedia.org/wiki/Antwone_Fisher",
                "antwone fisher",
                "",
                "",
            ),
            (
                "Wikipedia: Antwone Fisher (film)",
                "https://en.wikipedia.org/wiki/Antwone_Fisher_(film)",
                "antwone fisher",
                "",
                "film",
            ),
            (
                "Wikipedia: King Kong Groover",
                "https://en.wikipedia.org/wiki/King_Kong_Groover",
                "king kong groover",
                "",
                "",
            ),
            (
                "Wikipedia: King Kong (comics)",
                "https://en.wikipedia.org/wiki/King_Kong_(comics)",
                "king kong",
                "",
                "comics",
            ),
            (
                "Wikipedia: King Kong (1959 musical)",
                "https://en.wikipedia.org/wiki/King_Kong_(1959_musical)",
                "king kong",
                "1959",
                "musical",
            ),
            (
                "Wikipedia: King Kong (1933 film)",
                "https://en.wikipedia.org/wiki/King_Kong_(1933_film)",
                "king kong",
                "1933",
                "film",
            ),
            (
                "Wikipedia: King Kong (1976 film)",
                "https://en.wikipedia.org/wiki/King_Kong_(1976_film)",
                "king kong",
                "1976",
                "film",
            ),
        ],
        [TITLE, URL, CLEANED_TITLE, WIKI_YEAR, TYPE],
    )

    # Call the method with the input DataFrame and the url_column
    df = WikipediaData.add_cleaned_title_type_and_year_columns(wiki_df, URL).orderBy(
        "title"
    )
    df.show()
    df.printSchema()
    expected_df.orderBy("title").show()
    expected_df.printSchema()

    # Assert that the resulting DataFrame has the expected columns and values
    assert set(df.columns) == {TITLE, URL, CLEANED_TITLE, WIKI_YEAR, TYPE}
    assert df.count() == 8
    assert df.collect() == expected_df.orderBy("title").collect()
