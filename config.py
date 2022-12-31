import os

ROOT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "")
)


POSTGRES_JAR_FILEPATH = os.path.join(ROOT_DIR, "jars", "postgresql-42.5.1.jar")
SPARK_XML_JAR_FILEPATH = os.path.join(ROOT_DIR, "jars", "spark-xml_2.12-0.15.0.jar")
JARS_FILE_PATHS = f"{POSTGRES_JAR_FILEPATH},{SPARK_XML_JAR_FILEPATH}"


# Wikipedia Columns
URL = "url"
WIKI_YEAR = "year"
TYPE = "type"
ABSTRACT = "abstract"


# IMDB Columns
TITLE = "title"
GENRES = "genres"
PRODUCTION_COMPANIES = "production_companies"
RELEASE_DATE = "release_date"
VOTE_AVERAGE = "vote_average"
BUDGET = "budget"
REVENUE = "revenue"
RATING = "rating"
YEAR = "year"
REVENUE_TO_BUDGET = "revenue_to_budget"

# Common Columns
CLEANED_TITLE = "cleaned_title"
