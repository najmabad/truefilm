import os

ROOT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
)

INPUT_DATA_DIR = os.path.join(ROOT_DIR, "input_data")
WIKI_ABSTRACTS_FILEPATH = os.path.join(INPUT_DATA_DIR, "enwiki-latest-abstract.xml")
IMBD_MOVIES_METADATA_FILEPATH = os.path.join(INPUT_DATA_DIR, "movies_metadata.csv")

WIKI_ABSTRACTS_FILEPATH = (
    "/Users/najmabader/Projects/truefilm_etl/tests/data/enwiki-latest-abstract-test.xml"
)
IMBD_MOVIES_METADATA_FILEPATH = (
    "/Users/najmabader/Projects/truefilm_etl/tests/data/movies-metadata-test.csv"
)

# Wikipedia Columns
URL = "url"


# IMDB Columns
TITLE = "title"
GENRES = "genres"
PRODUCTION_COMPANIES = "production_companies"
RELEASE_DATE = "release_date"
VOTE_AVERAGE = "vote_average"
BUDGET = "budget"
REVENUE = "revenue"
RATING = "rating"

# Added Columns
YEAR = "year"
REVENUE_TO_BUDGET = "revenue_to_budget"
CLEANED_TITLE = "cleaned_title"
