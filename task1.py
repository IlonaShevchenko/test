import pyspark.sql.functions as f


def task1(title_akas_df):
    path = 'result/result1'
    result_df = title_akas_df.filter(f.col('region') == 'UA').select('title', 'region')
    result_df.write.csv(path, header=True, mode='overwrite')
    #result_df.show()
