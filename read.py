import pyspark.sql.types as t
import pyspark.sql.functions as f


def read_akas(spark_session):
    """
    Read 'data/title.akas.tsv.gz'-file in DataFrame
    Args:
        spark_session: Spark session
    Returns:
        DataFrame
    """
    akas_schema = t.StructType([t.StructField('titleId', t.StringType(), False),
                                t.StructField('ordering', t.IntegerType(), False),
                                t.StructField('title', t.StringType(), True),
                                t.StructField('region', t.StringType(), True),
                                t.StructField('language', t.StringType(), True),
                                t.StructField('types', t.StringType(), True),
                                t.StructField('attributes', t.StringType(), True),
                                t.StructField('isOriginalTitle', t.IntegerType(), True)
                                ])

    path = 'data/title.akas.tsv.gz'
    akas_df = spark_session.read.csv(path, sep=r'\t', header=True, nullValue='null', schema=akas_schema)
    akas_df = akas_df.withColumn('region', f.when(f.col('region') == '\\N', None).otherwise(f.col('region')))
    akas_df = akas_df.withColumn('language', f.when(f.col('language') == '\\N', None).otherwise(f.col('language')))
    akas_df = akas_df.withColumn('types', f.when(f.col('types') == '\\N', None).otherwise(f.col('types')))
    akas_df = akas_df.withColumn('attributes',
                                 f.when(f.col('attributes') == '\\N', None).otherwise(f.col('attributes')))
    return akas_df


def read_title_basic(spark_session):
    """
    Read 'data/title.basics.tsv.gz'-file in DataFrame
    Args:
        spark_session: Spark session
    Returns:
        DataFrame
    """
    title_basics_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                       t.StructField('titleType', t.StringType(), True),
                                       t.StructField('primaryTitle', t.StringType(), True),
                                       t.StructField('originalTitle', t.StringType(), True),
                                       t.StructField('isAdult', t.IntegerType(), True),
                                       t.StructField('startYear', t.IntegerType(), True),
                                       t.StructField('endYear', t.DateType(), True),
                                       t.StructField('runtimeMinutes', t.IntegerType(), True),
                                       t.StructField('genres', t.StringType(), True)
                                       ])

    path = 'data/title.basics.tsv.gz'
    title_basics_df = spark_session.read.csv(path, sep=r'\t', header=True, nullValue='null',
                                            schema=title_basics_schema)
    title_basics_df = title_basics_df.withColumn('genres', f.when(f.col('genres') == '\\N', None)
                                                 .otherwise(f.split(title_basics_df.genres, ',')))
    title_basics_df.show(truncate=False)
    return title_basics_df


def read_title_crew(spark_session):
    """
    Read 'data/title.crew.tsv.gz'-file in DataFrame
    Args:
        spark_session: Spark session
    Returns:
        DataFrame
    """
    title_crew_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                      t.StructField('directors', t.StringType(), True),
                                      t.StructField('writers', t.StringType(), True)
                                      ])

    path = 'data/title.crew.tsv.gz'
    title_crew_df = spark_session.read.csv(path, sep=r'\t', header=True, nullValue='null',
                                           schema=title_crew_schema)
    title_crew_df = title_crew_df.withColumn('writers', f.when(f.col('writers') == '\\N', None).
                                             otherwise(f.col('writers')))
    return title_crew_df


def read_title_episode(spark_session):
    """
    Read 'data/title.episode.tsv.gz'-file in DataFrame
    Args:
        spark_session: Spark session
    Returns:
        DataFrame
    """
    title_episode_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                         t.StructField('parentTconst', t.StringType(), True),
                                         t.StructField('seasonNumber', t.IntegerType(), True),
                                         t.StructField('episodeNumber', t.IntegerType(), True)
                                         ])

    path = 'data/title.episode.tsv.gz'
    title_episode_df = spark_session.read.csv(path, sep=r'\t', header=True, nullValue='null',
                                              schema=title_episode_schema)
    return title_episode_df


def read_title_principals(spark_session):
    """
    Read 'data/title.principals.tsv.gz'-file in DataFrame
    Args:
        spark_session: Spark session
    Returns:
        DataFrame
    """
    title_principals_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                            t.StructField('ordering', t.IntegerType(), True),
                                            t.StructField('nconst', t.StringType(), True),
                                            t.StructField('category', t.StringType(), True),
                                            t.StructField('job', t.StringType(), True),
                                            t.StructField('characters', t.StringType(), True)
                                            ])

    path = 'data/title.principals.tsv.gz'
    title_principals_df = spark_session.read.csv(path, sep=r'\t', header=True, nullValue='null',
                                                 schema=title_principals_schema)
    title_principals_df = title_principals_df.withColumn('job', f.when(f.col('job') == '\\N', None)
                                                         .otherwise(f.col('job')))
    title_principals_df = title_principals_df.withColumn('characters', f.when(f.col('characters') == '\\N', None)
                                                         .otherwise(f.col('characters')))
    return title_principals_df


def read_title_ratings(spark_session):
    """
    Read 'data/title.ratings.tsv.gz'-file in DataFrame
    Args:
        spark_session: Spark session
    Returns:
        DataFrame
    """
    title_ratings_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                         t.StructField('averageRating', t.DoubleType(), True),
                                         t.StructField('numVotes', t.IntegerType(), True)
                                         ])
    path = 'data/title.ratings.tsv.gz'
    title_ratings_df = spark_session.read.csv(path, sep=r'\t', header=True, nullValue='null',
                                              schema=title_ratings_schema)
    return title_ratings_df


def read_name_basics(spark_session):
    """
    Read 'data/name.basics.tsv.gz'-file in DataFrame
    Args:
        spark_session: Spark session
    Returns:
        DataFrame
    """
    name_basics_schema = t.StructType([t.StructField('nconst', t.StringType(), False),
                                       t.StructField('primaryName', t.StringType(), True),
                                       t.StructField('birthYear', t.IntegerType(), True),
                                       t.StructField('deathYear', t.IntegerType(), True),
                                       t.StructField('primaryProfession', t.StringType(), True),
                                       t.StructField('knownForTitles', t.StringType(), True)
                                       ])
    path = 'data/name.basics.tsv.gz'
    name_basics_df = spark_session.read.csv(path, sep=r'\t', header=True, nullValue='null',
                                            schema=name_basics_schema)
    return name_basics_df