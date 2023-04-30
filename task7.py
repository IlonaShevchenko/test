import pyspark.sql.functions as f


def task7(title_basics_df, title_ratings_df):
    path = '/result/result76'
    i = 1
    result_df = None
    for start in range(1970, 2000, 10):
        data_df = title_basics_df.filter((f.col('startYear') >= start) & (f.col('startYear') < start + 10))
        data_df = data_df.join(title_ratings_df, on='tconst', how='left').select('originalTitle', 'startYear',
                                                                                 'averageRating')
        data_df = data_df.orderBy('averageRating', ascending=False).limit(10)
        if i == 1:
            result_df = data_df
            i += 1
        else:
            result_df = result_df.union(data_df)
    # result_df.write.csv(path, header=True, mode='overwrite')
    result_df.show(truncate=False)
