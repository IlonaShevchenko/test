import pyspark.sql.functions as f


def task2(name_basics_df, century):
    """
    Get the list of people’s names, who were born in the _th century.
    Args:
        name_basics_df: dataframe from name.basics.tsv.gz
        century: century number
    Returns:
        csv-file with result of task in 'result/result2'
    """
    path = 'result/result2'
    start = (century-1)*100
    result_df = (name_basics_df.filter((f.col('birthYear') >= start) & (f.col('birthYear') <= start + 99))
        .select('primaryName', 'birthYear'))
    result_df.write.csv(path, header=True, mode='overwrite')
    # result_df.show()
