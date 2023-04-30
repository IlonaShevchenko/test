# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f
import read as r
import task1 as t1
import task2 as t2
import task3 as t3
import task4 as t4
import task5 as t5
import task6 as t6
import task7 as t7

def main():
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())

    title_akas_df = r.read_akas(spark_session)
    #title_akas_df.show(truncate=False)

    title_basics_df = r.read_title_basic(spark_session)
    #title_basics_df.show(truncate=False)

    title_crew_df = r.read_title_crew(spark_session)
    #title_crew_df.show(truncate=False)

    title_episode_df = r.read_title_episode(spark_session)
    #title_episode_df.show(truncate=False)

    title_principals_df = r.read_title_principals(spark_session)
    #title_principals_df.show(truncate=False)

    title_ratings_df = r.read_title_ratings(spark_session)
    #title_ratings_df.show(truncate=False)

    name_basics_df = r.read_name_basics(spark_session)
    #name_basics_df.show(truncate=False)

    print('TASK - 1')
    t1.task1(title_akas_df)

    print('TASK - 2')
    t2.task2(name_basics_df, 19)

    print('TASK - 3')
    t3.task3(title_basics_df, 'movie', 120)

    print('TASK - 4')
    t4.task4(title_principals_df, name_basics_df, title_basics_df)

    print('TASK - 5')
    t5.task5(title_basics_df, title_akas_df)

    print('TASK - 6')
    t6.task6(title_episode_df, title_basics_df)

    print('TASK - 7')
    t7.task7(title_basics_df, title_ratings_df)




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()







