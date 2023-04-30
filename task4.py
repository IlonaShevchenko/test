import pyspark.sql.functions as f


def task4(title_principals_df, name_basics_df, title_basics_df):
    path = '/result/result4'
    actors_df = (title_principals_df.filter(f.col('category') == 'actor')
        .select('tconst', 'nconst', 'category', 'characters'))
    actors_characters_df = (name_basics_df.join(actors_df, on='nconst', how='left')
        .select('primaryName', 'characters', 'tconst'))
    result_df = (actors_characters_df.join(title_basics_df, on='tconst', how='left')
        .select('primaryName', 'originalTitle', 'characters'))
    # result_df.write.csv(path, header=True, mode='overwrite')
    result_df.show(truncate=False)
