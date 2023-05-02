import pyspark.sql.functions as f


def task5(title_basics_df, title_akas_df):
    path = 'result/result5'
    adult_films_df = title_basics_df.filter((f.col('isAdult') == 1))
    result_df = adult_films_df.join(title_akas_df, adult_films_df.tconst == title_akas_df.titleId, how='left')
    result_df = result_df.groupBy('region').count().orderBy('count', ascending=False).limit(100)
    result_df.write.csv(path, header=True, mode='overwrite')
    # result_df.show(truncate=False)
