# Movie Analyser ETL Pipeline

## Requirements

- Docker
- Docker Compose
- Python 3.7+
- Dagster

## Setup

1. Clone the repository.
2. Navigate to the project directory.

   ```bash
   git clone https://github.com/sarkissian001/batch-processing.git
   cd batch-processing
   ```

## Running the Pipeline Locally

1. Create a virtual environment and activate it.

   ```shell
   python3 -m venv venv
   source venv/bin/activate
   ```

2. Install the dependencies:

   ```shell
     pip install -r requirements.txt
   ```
   
3. Run the Spark job locally:
   ```shell
     python3 app/batch_app.py
   ```
   
4. Run the pipeline using Dagit:

   ```shell
     dagit -f app/pipeline.py -h 0.0.0.0 -p 3000
   ```
   
5. Access the Dagit UI by navigating to http://localhost:3000 in your web browser.

## Running the Pipeline with Docker

1. Build and run the Docker containers:

   ```shell
   docker-compose up --build
   ```
   
2. Access the Dagit UI by navigating to http://localhost:3000 in your web browser.


## Run Tests

1. Navigate to the project directory:

   ```shell
   cd /batch-processing && python3 -m unittest
   ```
