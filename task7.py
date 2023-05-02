import pyspark.sql.functions as f


def task7(title_basics_df, title_ratings_df, n):
    """
    Get n titles of the most popular movies/series etc. by each decade.
    Args:
        title_basics_df: dataframe from name.basics.tsv.gz
        title_ratings_df: dataframe from title.ratings.tsv.gz
        n: top titles
    Returns:
        csv-file with result of task in 'result/result7'
    """
    path = 'result/result7'
    i = 1
    result_df = None
    for start in range(1950, 2020, 10):
        data_df = title_basics_df.filter((f.col('startYear') >= start) & (f.col('startYear') <= start + 9))
        data_df = data_df.withColumn('decade', f.lit(str(start) + '-' + str(start + 9)))
        data_df = data_df.join(title_ratings_df, on='tconst', how='left').select('decade', 'originalTitle', 'startYear',
                                                                                 'averageRating')
        data_df = data_df.orderBy('averageRating', ascending=False).limit(10)
        if i == 1:
            result_df = data_df
            i += 1
        else:
            result_df = result_df.union(data_df)
    result_df.write.csv(path, header=True, mode='overwrite')
    # result_df.show(100, truncate=False)
