from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg, collect_list, collect_set, struct, expr
import os
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataLoader:
    """Loads data from specified path."""

    def __init__(self, spark: SparkSession, data_path: str):
        self.spark = spark
        self.data_path = data_path

    def load_delimited_file(self, filename: str) -> DataFrame:
        """Loads a CSV file into a DataFrame."""
        logger.info(f"Loading data from {filename}...")
        return (
            self.spark.read.option("header", "true")
            .option("delimiter", "\t")
            .csv(os.path.join(self.data_path, filename))
        )


class MovieProcessor:
    """Processes movie-related data."""

    def __init__(
        self,
        titles_df: DataFrame,
        ratings_df: DataFrame,
        principals_df: DataFrame,
        name_basics_df: DataFrame,
        title_akas_df: DataFrame,
    ):
        self.titles_df = titles_df
        self.ratings_df = ratings_df
        self.principals_df = principals_df
        self.name_basics_df = name_basics_df
        self.title_akas_df = title_akas_df

    def filter_ratings(self, min_votes: int = 500) -> DataFrame:
        """Filters ratings DataFrame to include movies with at least `min_votes` votes."""
        logger.info("Filtering ratings...")
        return self.ratings_df.filter(col("numVotes") >= min_votes)

    @staticmethod
    def calculate_average_votes(ratings_filtered_df: DataFrame) -> float:
        """Calculates and returns the average number of votes."""
        logger.info("Calculating average number of votes...")
        return ratings_filtered_df.agg(
            avg(col("numVotes")).alias("averageVotes")
        ).collect()[0]["averageVotes"]

    @staticmethod
    def calculate_ranking_score(
        ratings_filtered_df: DataFrame, average_num_votes: float
    ) -> DataFrame:
        """Calculates and adds a ranking score to the ratings DataFrame."""
        logger.info("Calculating ranking scores...")
        return ratings_filtered_df.withColumn(
            "rankingScore", (col("numVotes") / average_num_votes) * col("averageRating")
        )

    @staticmethod
    def get_top_movies(ratings_with_score_df: DataFrame, top_n: int) -> DataFrame:
        """Returns the top N movies based on the ranking score."""
        logger.info(f"Selecting top {top_n} movies...")
        return ratings_with_score_df.orderBy(col("rankingScore").desc()).limit(top_n)

    def get_movie_titles(self, top_movies_df: DataFrame) -> DataFrame:
        """Joins top movies with titles DataFrame to get movie titles."""
        logger.info("Joining top movies with titles...")
        return top_movies_df.join(self.titles_df, "tconst").select(
            top_movies_df["tconst"],
            self.titles_df["primaryTitle"],
            top_movies_df["rankingScore"],
        )

    def get_most_credited_persons(self) -> DataFrame:
        """Aggregates the most credited persons for each movie."""
        logger.info("Aggregating most credited persons...")
        principals_with_names_df = self.principals_df.join(
            self.name_basics_df, "nconst"
        )
        most_credited_persons_df = principals_with_names_df.groupBy(
            "tconst", "primaryName"
        ).count()
        return most_credited_persons_df.groupBy("tconst").agg(
            collect_list(struct("primaryName", "count")).alias("mostCreditedPersons")
        )

    @staticmethod
    def sort_and_filter_credits(
        df: DataFrame, credit_count_threshold: int
    ) -> DataFrame:
        """Sorts and filters credited persons based on the credit count threshold."""
        logger.info("Sorting and filtering credited persons...")
        df = df.withColumn(
            "mostCreditedPersons",
            expr(
                "array_sort(mostCreditedPersons, (left, right) -> case when left.count > right.count then -1 when left.count < right.count then 1 else 0 end)"
            ),
        )
        if credit_count_threshold > 0:
            df = df.withColumn(
                "mostCreditedPersons",
                expr(
                    f"filter(mostCreditedPersons, x -> x.count > {credit_count_threshold})"
                ),
            )
        return df

    def get_different_titles(self) -> DataFrame:
        """Aggregates different titles for each movie."""
        logger.info("Aggregating different titles...")
        return self.title_akas_df.groupBy("titleId").agg(
            collect_set("title").alias("differentTitles")
        )


class MovieAggregator:
    """Aggregates all movie information."""

    def __init__(self, movie_processor: MovieProcessor):
        self.movie_processor = movie_processor

    def aggregate_movie_data(
        self, credit_count_threshold: int, top_n_movies: int
    ) -> DataFrame:
        """Aggregates movie data including titles, credits, and different titles."""
        logger.info("Aggregating movie data...")
        ratings_filtered_df = self.movie_processor.filter_ratings()
        average_num_votes = self.movie_processor.calculate_average_votes(
            ratings_filtered_df
        )
        ratings_with_score_df = self.movie_processor.calculate_ranking_score(
            ratings_filtered_df, average_num_votes
        )
        top_movies_df = self.movie_processor.get_top_movies(
            ratings_with_score_df, top_n_movies
        )
        top_movies_with_titles_df = self.movie_processor.get_movie_titles(top_movies_df)
        most_credited_persons_df = self.movie_processor.get_most_credited_persons()
        top_movies_with_credits_df = top_movies_with_titles_df.join(
            most_credited_persons_df, "tconst", "left"
        )
        sorted_most_credited_persons_df = self.movie_processor.sort_and_filter_credits(
            top_movies_with_credits_df, credit_count_threshold
        )
        different_titles_df = self.movie_processor.get_different_titles()
        return sorted_most_credited_persons_df.join(
            different_titles_df,
            sorted_most_credited_persons_df["tconst"] == different_titles_df["titleId"],
            "left",
        ).select(
            top_movies_with_titles_df["tconst"],
            "primaryTitle",
            "rankingScore",
            "mostCreditedPersons",
            "differentTitles",
        )


class MovieDataProcessor:
    """Runs the IMDB Data Processing Application."""

    def __init__(
        self,
        spark: SparkSession,
        data_path: str,
        credit_count_threshold: int,
        top_n_movies: int,
    ):

        self.spark = spark
        self.data_path = data_path
        self.credit_count_threshold = credit_count_threshold
        self.top_n_movies = top_n_movies

        # initialise empty sdfs
        self.title_akas_df = None
        self.name_basics_df = None
        self.principals_df = None
        self.ratings_df = None
        self.titles_df = None

    def load_data(self):
        """Loads all required data into DataFrames."""
        logger.info("Loading data...")
        data_loader = DataLoader(self.spark, self.data_path)
        self.titles_df = data_loader.load_delimited_file("title.basics.tsv")
        self.ratings_df = data_loader.load_delimited_file("title.ratings.tsv")
        self.principals_df = data_loader.load_delimited_file("title.principals.tsv")
        self.name_basics_df = data_loader.load_delimited_file("name.basics.tsv")
        self.title_akas_df = data_loader.load_delimited_file("title.akas.tsv")

    def process_and_aggregate(self) -> DataFrame:
        """Processes and aggregates movie data."""
        logger.info("Processing and aggregating data...")
        movie_processor = MovieProcessor(
            self.titles_df,
            self.ratings_df,
            self.principals_df,
            self.name_basics_df,
            self.title_akas_df,
        )
        movie_aggregator = MovieAggregator(movie_processor)
        return movie_aggregator.aggregate_movie_data(
            self.credit_count_threshold, self.top_n_movies
        ).orderBy(col("rankingScore").desc())

    def run(self) -> DataFrame:
        """Runs the IMDB Data Processing Application."""
        try:
            self.load_data()
            top_ten_movies_with_all_info_df = self.process_and_aggregate()
            top_ten_movies_with_all_info_df.show(self.top_n_movies)
            return top_ten_movies_with_all_info_df
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            raise e
