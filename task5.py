import pyspark.sql.functions as f


def task5(title_basics_df, title_akas_df, n):
    """
    Get information about how many adult movies/series etc. there are per region.
    Get the top n of them from the region with the biggest count to the region with the smallest one.
    Args:
        title_basics_df: dataframe from name.basics.tsv.gz
        title_akas_df: dataframe from title.akas.tsv.gz
        n: top region
    Returns:
        csv-file with result of task in 'result/result5'
    """
    path = 'result/result5'
    adult_films_df = title_basics_df.filter((f.col('isAdult') == 1))
    result_df = adult_films_df.join(title_akas_df, adult_films_df.tconst == title_akas_df.titleId, how='left')
    result_df = result_df.groupBy('region').count().orderBy('count', ascending=False).limit(n)
    result_df.write.csv(path, header=True, mode='overwrite')
    # result_df.show(truncate=False)
