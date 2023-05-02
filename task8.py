import pyspark.sql.functions as f


def task8(title_basics_df, title_ratings_df, n):
    """
    Get n titles of the most popular movies/series etc. by each genre.
    Args:
        title_basics_df: dataframe from name.basics.tsv.gz
        title_ratings_df: dataframe from title.ratings.tsv.gz
        n: top titles
    Returns:
        csv-file with result of task in 'result/result8'
    """
    path = 'result/result8'
    genres = ['Comedy', 'Adventure', 'Drama', 'Animation', 'Family', 'Short']
    i = 1
    result_df = None
    for g in genres:
        data_df = title_basics_df.select('tconst', 'originalTitle',
                                           f.explode(title_basics_df.genres).alias('genre')).filter(f.col('genre') == g)
        data_df = data_df.join(title_ratings_df, on='tconst', how='left').select('originalTitle', 'genre',
                                                                                     'averageRating')
        data_df = data_df.orderBy('averageRating', ascending=False).limit(10)
        if i == 1:
            result_df = data_df
            i += 1
        else:
            result_df = result_df.union(data_df)

    result_df.write.csv(path, header=True, mode='overwrite')
    # result_df.show(60, truncate=False)
