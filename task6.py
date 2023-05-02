import pyspark.sql.functions as f


def task6(title_episode_df, title_basics_df, n):
    """
    Get information about how many episodes in each TV Series.
    Get the top n of them starting from the TV Series with the biggest quantity of episodes.
    Args:
        title_episode_df: dataframe from title.episode.tsv.gz
        title_basics_df: dataframe from name.basics.tsv.gz
        n: top regions
    Returns:
        csv-file with result of task in 'result/result6'
    """
    path = 'result/result6'
    result_df = title_episode_df.groupBy('parentTconst').count()
    result_df = result_df.join(title_basics_df, title_basics_df.tconst == result_df.parentTconst,
                                           how='left').select('originalTitle', 'count')
    result_df = result_df.orderBy('count', ascending=False).limit(n)
    result_df.write.csv(path, header=True, mode='overwrite')
    # result_df.show(truncate=False)
