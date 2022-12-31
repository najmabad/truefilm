#!/bin/bash

COMMAND=${1}


docker_compose_up() {
  if [[ ! -f .env ]]; then
    echo "Error: .env file not found. Please make sure that the .env file exists in the current directory and try again."
    return 1
  fi

  set -o allexport
  source .env

  echo "Checking status of Docker containers..."
  if ! docker-compose up -d ; then
    if [[ "$?" -eq 1 ]]; then
      echo "Docker stack started successfully."
    else
      echo "Error: An unknown error occurred while starting the Docker containers. Please check the output above for more information."
      return 1
    fi
  else
    echo "Docker containers are already running."
  fi
}

run_etl() {
  # Check for the presence of the myenv folder and python3.9 executable
  if [[ ! -d myenv ]] || [[ ! -x myenv/bin/python3.9 ]]; then
    echo "Error: myenv folder or python3.9 executable not found"
    return 1
  fi

  # Check if the Docker containers are running
  if ! docker-compose ps | grep -q "Up"; then
    # Start the Docker containers if they are not already running
    docker_compose_up
  fi

  # Check for the presence of the input_data folder
  files=(movies_metadata.csv enwiki-latest-abstract.xml)

  for input_file in "${files[@]}"; do
      if [[ -e "./input_data/${input_file}" ]]; then
        echo "Input file: ${input_file} present"
      else
        echo "Input file: ${input_file}... does not exist. Exiting"
        exit 1
      fi
      done

  # Run the run.py script using python3.9 from the myenv folder
  echo "Running run.py with Python $(myenv/bin/python3.9 --version)..."
  myenv/bin/python3.9 run.py "./input_data/movies_metadata.csv" "./input_data/enwiki-latest-abstract.xml"
}

sample_etl() {
  # Check for the presence of the myenv folder and python3.9 executable
  if [[ ! -d myenv ]] || [[ ! -x myenv/bin/python3.9 ]]; then
    echo "Error: myenv folder or python3.9 executable not found"
    return 1
  fi

  # Check if the Docker containers are running
  if ! docker-compose ps | grep -q "Up"; then
    # Start the Docker containers if they are not already running
    docker_compose_up
  fi

  # Check for the presence of the input_data folder
  files=(movies-metadata-test.csv enwiki-latest-abstract-test.xml)

  for input_file in "${files[@]}"; do
      if [[ -e "./tests/data/${input_file}" ]]; then
        echo "Input file: ${input_file} present"
      else
        echo "Input file: ${input_file}... does not exist. Exiting"
        exit 1
      fi
      done

  # Run the run.py script using python3.9 from the myenv folder
  echo "Running run.py with Python $(myenv/bin/python3.9 --version)..."
  myenv/bin/python3.9 run.py "./tests/data/movies-metadata-test.csv" "./tests/data/enwiki-latest-abstract-test.xml"
}


docker_compose_down() {
  echo "Stopping Docker containers..."
  if ! docker-compose down; then
    if [[ "$?" -eq 0 ]]; then
      echo "Successfully stopped Docker containers"
    else
      echo "Error: Failed to stop Docker containers"
      return 1
    fi
  else
    echo "Docker containers are not running"
  fi
}



print_help() {
  echo "Usage: truefilm-cli.sh [COMMAND]"
  echo ""
  echo "Commands:"
  echo "  start           Start the Docker stack"
  echo "  stop            Stop the Docker stack"
  echo "  run             Run the ETL process with the default input data"
  echo "  run-sample-etl  Run the ETL process with sample input data for testing"
  echo "  help            Print this help message"
}

case "${COMMAND}" in
start)
  docker_compose_up
  ;;
stop)
  docker_compose_down
  ;;
run)
  run_etl
  ;;
run-sample-etl)
  sample_etl
  ;;
help)
  print_help
  ;;
*)
  echo "Error: Invalid command. Use 'truefilm-cli.sh help' for a list of available commands."
  ;;
esac
