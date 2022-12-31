import sys

from config import REVENUE_TO_BUDGET
from truefilm_etl.etl.match_data import IMDBProcessor, WikipediaProcessor, MatchData
from truefilm_etl.utils.postgres import PostgresDAO

import logging.config

logging.config.fileConfig(fname="logging.config", disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)


def main(imdb_file_path: str, wikipedia_file_path: str):
    try:
        logger.info("🚀 Starting ETL process")
        # Get the dataframes
        logger.info("🌟 STEP 1: Processing IMDB and Wikipedia data")
        imdb_df = IMDBProcessor(imdb_file_path).df()
        logger.info(f"{imdb_df.count()} rows in IMDB dataframe")
        wikipedia_df = WikipediaProcessor(wikipedia_file_path).df()
        logger.info(f"{wikipedia_df.count()} rows in Wikipedia dataframe")
        logger.info("✅  STEP 1: Completed \n")

        # Match the dataframes
        logger.info("🌟 STEP 2: Matching dataframes")
        match_dataframes = MatchData(imdb_df, wikipedia_df)
        matched_imbd_and_wiki = match_dataframes.match()
        logger.info("✅  STEP 2: Completed \n")

        # Write the dataframe to a PostgreSQL database
        logger.info("🌟 STEP 3: Writing matched dataframes to PostgreSQL")
        postgres_dao = PostgresDAO()
        postgres_dao.write_to_table(
            matched_imbd_and_wiki, "movies", order_by_col=REVENUE_TO_BUDGET, rows=1000
        )
        logger.info("✅  STEP 3: Completed \n")

        logger.info("🎉 Finished ETL process")

    except Exception as e:
        logger.error("❌ Error running ETL process")
        logger.error(e)


if __name__ == "__main__":
    # Read the input file paths from the command line arguments
    imdb_file_path = sys.argv[1]
    wikipedia_file_path = sys.argv[2]

    main(imdb_file_path, wikipedia_file_path)
