# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f
import read as r


def main():
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())

    akas_df = r.read_akas(spark_session)
    akas_df.show(truncate=False)

    title_basic_df = r.read_title_basic(spark_session)
    title_basic_df.show(truncate=False)

    title_crew_df = r.read_title_crew(spark_session)
    title_crew_df.show(truncate=False)

    title_episode_df = r.read_title_episode(spark_session)
    title_episode_df.show(truncate=False)

    title_principals_df = r.read_title_principals(spark_session)
    title_principals_df.show(truncate=False)

    title_ratings_df = r.read_title_ratings(spark_session)
    title_ratings_df.show(truncate=False)

    name_basics_df = r.read_name_basics(spark_session)
    name_basics_df.show(truncate=False)



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()







