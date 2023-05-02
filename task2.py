import pyspark.sql.functions as f


def task2(name_basics_df, century):
    path = 'result/result2'
    start = (century-1)*100
    result_df = (name_basics_df.filter((f.col('birthYear') >= start) & (f.col('birthYear') <= start + 99))
        .select('primaryName', 'birthYear'))
    result_df.write.csv(path, header=True, mode='overwrite')
    # result_df.show()
