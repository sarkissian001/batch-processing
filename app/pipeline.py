import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dagster import asset, Definitions, AssetSelection, define_asset_job, Output, MetadataValue
from dagster_pyspark import pyspark_resource


from app.download_datasets import download_and_extract
from app.batch_app import MovieDataProcessor


@asset
def download_ibm_movies_tsv_files(context):
    """
    Returns the path of the folder containing the downloaded movies TSV files
    """
    data_dir = os.path.join(os.path.dirname(__file__), "..", ".data")
    data_dir = os.path.abspath(data_dir)

    # List of files to download
    files = [
        "name.basics.tsv.gz",
        "title.akas.tsv.gz",
        "title.basics.tsv.gz",
        "title.principals.tsv.gz",
        "title.ratings.tsv.gz"
    ]

    context.log.info("Starting to download movie files")
    with ThreadPoolExecutor(max_workers=len(files)) as executor:
        future_to_file = {executor.submit(download_and_extract, file, data_dir): file for file in files}
        for future in as_completed(future_to_file):
            file = future_to_file[future]
            try:
                future.result()
                context.log.info(f"{file=} downloaded and extracted.")
            except Exception as exc:
                context.log.error(f"{file=} exception: {exc}")

    files = [f.replace(".gz", "") for f in files]
    return Output(
        value=data_dir,
        metadata={"downloaded_files": MetadataValue.md(str(files))}
    )


@asset(required_resource_keys={"pyspark"})
def analyse_top_ten_imb_movies(context, download_ibm_movies_tsv_files):
    """
    Writes the analysed data to the disk and prints the top 10 movies
    """
    spark = context.resources.pyspark.spark_session

    data_path = download_ibm_movies_tsv_files
    app = MovieDataProcessor(spark, data_path, credit_count_threshold=1, top_n_movies=10)

    result_df = app.run()
    context.log.info("Data processed and aggregated.")

    # Ensure result_df is not None before converting to pandas
    if result_df is None:
        raise Exception("Result DataFrame is None")

    # Convert PySpark DataFrame to pandas DataFrame
    result_pdf = result_df.toPandas()

    # Save the pandas DataFrame to a CSV file
    output_path = os.path.join(data_path, "processed_movies.csv")
    result_pdf.to_csv(output_path, index=False)

    return Output(
        value=result_pdf,
        metadata={"df_head": MetadataValue.md(result_pdf.head(10).to_markdown())}
    )


defs = Definitions(
    assets=[download_ibm_movies_tsv_files, analyse_top_ten_imb_movies],
    resources={
        "pyspark": pyspark_resource.configured({
            "spark_conf": {
                "spark.executor.memory": "4g",
                "spark.driver.memory": "4g"
            }
        })
    },
    jobs=[
        define_asset_job(
            name="movie_analyser",
            selection=AssetSelection.assets(download_ibm_movies_tsv_files, analyse_top_ten_imb_movies),
        )
    ],
)

