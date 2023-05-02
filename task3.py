import pyspark.sql.functions as f


def task3(title_basics_df, type_film, t):
    path = 'result/result3'
    result_df = (title_basics_df.filter((f.col('titleType') == type_film) & (f.col('runtimeMinutes') > t))
        .select('originalTitle', 'runtimeMinutes', 'titleType'))
    result_df.write.csv(path, header=True, mode='overwrite')
    # result_df.show(truncate=False)

