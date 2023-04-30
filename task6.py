import pyspark.sql.functions as f


def task6(title_episode_df, title_basics_df):
    path = '/result/result6'
    result_df = title_episode_df.groupBy('parentTconst').count()
    result_df = result_df.join(title_basics_df, title_basics_df.tconst == result_df.parentTconst,
                                           how='left').select('originalTitle', 'count')
    result_df = result_df.orderBy('count', ascending=False).limit(50)
    # result_df.write.csv(path, header=True, mode='overwrite')
    result_df.show(truncate=False)
