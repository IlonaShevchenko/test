import pyspark.sql.functions as f


def task3(title_basics_df, film_type, t):
    """
    Get titles of all movies that last more than 2 hours.
    Args:
        title_basics_df: dataframe from title.basics.tsv.gz
        film_type: film type (e.g. 'movie')
        t: time in minutes
    Returns:
        csv-file with result of task in 'result/result3'
    """
    path = 'result/result3'
    result_df = (title_basics_df.filter((f.col('titleType') == film_type) & (f.col('runtimeMinutes') > t))
        .select('originalTitle', 'runtimeMinutes', 'titleType'))
    result_df.write.csv(path, header=True, mode='overwrite')
    # result_df.show(truncate=False)

