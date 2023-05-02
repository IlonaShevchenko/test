import pyspark.sql.functions as f


def task1(title_akas_df):
    """
    Get all titles of series/movies etc. that are available in Ukrainian.
    Args:
        title_akas_df: dataframe from title.akas.tsv.gz
    Returns:
        csv-file with result of task in 'result/result1'
    """
    path = 'result/result1'
    result_df = title_akas_df.filter(f.col('region') == 'UA').select('title', 'region')
    result_df.write.csv(path, header=True, mode='overwrite')
    #result_df.show()
