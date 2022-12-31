# Truefilm

## Project Overview
This project aims to provide insights about movies using two datasets:

- movies_metadata: a dataset containing various information about movies, sourced from iMDB.com
- enwiki-latest-abstract: a dataset containing the title, abstract, URL, and links for each Wikipedia article

The project's ETL (extract, transform, load) process processes these datasets and outputs a table called "movies", which allows users to perform queries to understand the top performing genres and production companies.
Additionally, users can easily navigate to the corresponding Wikipedia page for each film to read more in-depth about interesting films.

You can check the `notebooks/Example queries.ipynb` notebook to see how top performing genres and production companies can be queried.
You can run a notebook at `localhost:8888` with token = `mypassword`

## Project Requirements
- Docker
- enwiki-latest-abstract.xml data file
- movies_metadata.csv data file
- Python 3.9

### Docker
Docker installation depends on your operating system. You can find more details [here](https://docs.docker.com/get-docker/).

### Datafiles
Download the following two datasets:
- https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml.gz 
- https://www.kaggle.com/rounakbanik/the-movies-dataset/version/7#movies_metadata.csv

And place them in the `input_data` folder (you need to create it). Make sure to also unzip the `enwiki-latest-abstract.xml.gz` file.

### Python
This project requires Python 3.9.
Make sure you have Python 3.9 installed on your machine. You can check the version of Python you have installed by running python --version in the terminal.

## How To Run
Create a virtual environment using Python 3.9. This can be done by running the following command:

```
python3.9 -m venv myenv
```

Activate the virtual environment by running the following command:
```
source myenv/bin/activate
```

Install the required packages by running the following command:

```
pip install -r requirements.txt
```

To run the project, use the following command:

`docker-compose up`

this is needed just one time. 
Once the containers are initialised a first time they can also be run in detached mode.
This seems to be due to a bug that makes PostgreSQL container failed if it is just run in detached mode.
If you encounter any issues when running the project, you may need to run docker-compose up again.


To run the project after it has been set up with `docker-compose up`, you can use the `truefilm-cli.sh` script with the following commands:

- `./truefilm_cli.sh start`: start the Docker stack
- `./truefilm_cli.sh stop`: stop the Docker stack
- `./trueflim_cli.sh run-sample-etl` : run the ETL process with sample input data for testing
- `./truefilm_cli.sh run`: run the ETL (extract, transform, load) process with the default input data
- `./truefilm_cli.sh help`: print a help message with a list of available commands

For example, to start the Docker stack in detached mode, you can run `./truefilm-cli.sh start`. This will start the containers in the background and leave them running until you stop them with `./truefilm-cli.sh stop`.


When you are done working on the project, you can deactivate the virtual environment by running the following command:

```
deactivate
```

## Infrastructure Overview

This project's architecture consists of several Docker containers that work together to provide a distributed processing environment for data analytics tasks. The containers include:

- Spark Master: This container runs a Spark master node, which is responsible for managing the Spark cluster and coordinating the work of the worker nodes.
- Spark Workers: These containers run Spark worker nodes, which execute the tasks assigned to them by the master node. There are two worker nodes in this setup.
- Postgres: This container runs a PostgreSQL database server and is used to store the output data.
- Jupyter Notebook: This container runs a Jupyter Notebook server, which allows you to interactively explore and analyze data using the Spark cluster.

The containers are configured and launched using Docker Compose, as defined in the `docker-compose.yml` file. 
The Spark master and worker nodes communicate with each other using the Spark master URL specified in the SPARK_MASTER_URL environment variable. The Postgres container is configured using the environment variables POSTGRES_DB, POSTGRES_USER, and POSTGRES_PASSWORD. The Jupyter and PgAdmin containers are both configured to use the Spark master URL and the Postgres database server.


## Tech Stack
This project uses the following technology stack:

- Apache Spark: Apache Spark is a powerful open-source data processing engine that is used to perform fast calculations by caching datasets in memory and distributing the workload across a cluster of worker nodes. In this project, Apache Spark was chosen as the data processing framework due to its wide adoption and large user community.
- Spark-XML: This is an extension library for Spark that provides the ability to read and parse XML data sources.
- PostgreSQL JDBC Driver: The PostgreSQL JDBC driver is a Java library that allows Spark to communicate with and send data to a PostgreSQL database.

These dependencies were necessary to implement the data processing and analysis in this project. In particular, Apache Spark was used to process a large Wikipedia file.


## Matching Strategy

In order to match the IMDB dataset with the Wikipedia pages, we have identified four main scenarios:

1. No match. In this case, the row is kept as is.
2. One match. In this case, the row is matched to a single Wikipedia row.
3. More than one match where one is a film and the others are not. In this case, the row is matched to the Wikipedia row that is a film.
4. More than one match where there are multiple films. In this case, the row is matched to the Wikipedia row that is a film and has the same year as the IMDB row. 

To implement this logic, the function filters the Wikipedia data to keep only rows that match the conditions for each of the four scenarios. For example, to implement scenario 1, the function filters the Wikipedia data to keep only rows where the "wiki_year" column is empty and the "type" column is an empty string. To implement scenario 4, the function filters the Wikipedia data to keep only rows where the "wiki_year" column is not empty and the "type" column is equal to "film".

Next, the function performs a join between the filtered Wikipedia data and the IMDB data, using the appropriate join type (inner or left) and matching on the appropriate columns (either "cleaned_title" and "year" or "cleaned_title" only). The resulting dataframes are then combined using the union function and a subset of columns is selected to keep using the select function.

Overall, this matching strategy aims to maximize the number of matched rows while ensuring that each IMDB row is matched to the most relevant Wikipedia row, based on the conditions specified for each scenario.

It is worth noting that further exploration could be done using fuzzy matching techniques, such as those provided by the "fuzzywuzzy" library, to improve the matching accuracy. However, based on the given information, it seems that the current matching strategy is already producing satisfactory results.



## Test
To run the test `pytest` needs to be downloaded. You can do this by running the following command:

```
pip install -r requirements-dev.txt
```
then you can run:

```pytest .```

## Troubleshooting
If you get an error linked to the postgres container when running the `./truefilm_cli.sh run-sample-etl` or `./truefilm_cli.sh run` command, try first running `docker-compose up` in the terminal.
Once the containers are initialized a first time, you can stop them and run the `./truefilm_cli.sh`

## Further improvements
- Privacy and security
- Deploy the application with Kubernetes
- Explore different matching techniques
- Add more tests
- Add `great_expectations` library to test for data correctness
