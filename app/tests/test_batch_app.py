import unittest
import pandas as pd
from pyspark.sql.functions import col
import os
from pyspark.sql import SparkSession

from ..batch_app import MovieProcessor, MovieAggregator

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"


class TestMovieProcessing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("Test Movie Analyser") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()

        cls.test_data = {
            "titles": [
                {"tconst": "tt0000001", "primaryTitle": "Test Movie 1"},
                {"tconst": "tt0000002", "primaryTitle": "Test Movie 2"},
            ],
            "ratings": [
                {"tconst": "tt0000001", "averageRating": 5.6, "numVotes": 600},
                {"tconst": "tt0000002", "averageRating": 7.2, "numVotes": 800},
            ],
            "principals": [
                {"tconst": "tt0000001", "nconst": "nm0000001"},
                {"tconst": "tt0000001", "nconst": "nm0000002"},
                {"tconst": "tt0000002", "nconst": "nm0000003"},
                {"tconst": "tt0000001", "nconst": "nm0000001"},
            ],
            "name_basics": [
                {"nconst": "nm0000001", "primaryName": "Socrates"},
                {"nconst": "nm0000002", "primaryName": "Aristotle"},
                {"nconst": "nm0000003", "primaryName": "Xenophanes"},
            ],
            "title_akas": [
                {"titleId": "tt0000001", "title": "ThisIsTestMovieOne's AnotherTitle"},
                {"titleId": "tt0000002", "title": "ThisIsTestMovieTwo's SecondTitle"},
                {"titleId": "tt0000002", "title": "ThisIsTestMovieTwo's ThirdTitle"},
            ],
        }

    def setUp(self):
        self.titles_df = self.spark.createDataFrame(self.test_data["titles"])
        self.ratings_df = self.spark.createDataFrame(self.test_data["ratings"])
        self.principals_df = self.spark.createDataFrame(self.test_data["principals"])
        self.name_basics_df = self.spark.createDataFrame(self.test_data["name_basics"])
        self.title_akas_df = self.spark.createDataFrame(self.test_data["title_akas"])

        self.movie_processor = MovieProcessor(
            self.titles_df,
            self.ratings_df,
            self.principals_df,
            self.name_basics_df,
            self.title_akas_df,
        )
        self.movie_aggregator = MovieAggregator(self.movie_processor)

    def test_show_all_people_given_credits_for_the_movies(self):
        got_df = self.movie_aggregator.aggregate_movie_data(
            credit_count_threshold=0, top_n_movies=5
        ).orderBy(col("rankingScore").desc())
        sorted_col_names = sorted(got_df.columns)

        expected_data = [
            {
                "tconst": "tt0000002",
                "primaryTitle": "Test Movie 2",
                "rankingScore": 8.228571428571428,
                "mostCreditedPersons": [("Xenophanes", 1)],
                "differentTitles": [
                    "ThisIsTestMovieTwo's ThirdTitle",
                    "ThisIsTestMovieTwo's SecondTitle",
                ],
            },
            {
                "tconst": "tt0000001",
                "primaryTitle": "Test Movie 1",
                "rankingScore": 4.8,
                "mostCreditedPersons": [("Socrates", 2), ("Aristotle", 1)],
                "differentTitles": ["ThisIsTestMovieOne's AnotherTitle"],
            },
        ]

        expected_df = self.spark.createDataFrame(expected_data)
        got_df = got_df.select(*sorted_col_names)
        expected_df = expected_df.select(*sorted_col_names)

        got_df.show(truncate=False)
        expected_df.show(truncate=False)

        expected_pdf = expected_df.toPandas()
        got_pdf = got_df.toPandas()

        pd.testing.assert_frame_equal(expected_pdf, got_pdf, check_dtype=False)

    def test_show_all_people_given_credits_for_the_movies_where_credit_is_greater_than_one(
        self,
    ):
        got_df = self.movie_aggregator.aggregate_movie_data(
            credit_count_threshold=1, top_n_movies=5
        ).orderBy(col("rankingScore").desc())
        sorted_col_names = sorted(got_df.columns)

        expected_data = [
            {
                "tconst": "tt0000002",
                "primaryTitle": "Test Movie 2",
                "rankingScore": 8.228571428571428,
                "mostCreditedPersons": [],
                "differentTitles": [
                    "ThisIsTestMovieTwo's ThirdTitle",
                    "ThisIsTestMovieTwo's SecondTitle",
                ],
            },
            {
                "tconst": "tt0000001",
                "primaryTitle": "Test Movie 1",
                "rankingScore": 4.8,
                "mostCreditedPersons": [("Socrates", 2)],
                "differentTitles": ["ThisIsTestMovieOne's AnotherTitle"],
            },
        ]

        expected_df = self.spark.createDataFrame(expected_data)
        got_df = got_df.select(*sorted_col_names)
        expected_df = expected_df.select(*sorted_col_names)

        got_df.show(truncate=False)
        expected_df.show(truncate=False)

        expected_pdf = expected_df.toPandas()
        got_pdf = got_df.toPandas()

        pd.testing.assert_frame_equal(expected_pdf, got_pdf, check_dtype=False)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == "__main__":
    unittest.main()
