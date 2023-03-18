# SEEK AIPS Code Challenge

## Setup/Installation

The project is configured using Python Poetry to manage the virtual environment and dependencies.

For spark dependencies, Java and Scala will also need to be installed:
```bash
apt-get install -y default-jdk scala
```

To set up the environment for the first time from the lockfile, run `poetry install`

## Running the program and testing

To run all tests, use `poetry run pytest` from the project root dir.

To run the program in full, use `./scripts/run-job.sh` to start the spark job.
