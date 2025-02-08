import pyspark.sql.functions as F
import os

STORAGE_ACCOUNT = os.getenv('STORAGE_ACCOUNT')
STORAGE_ACCOUNT_KEY = os.getenv('STORAGE_ACCOUNT_KEY')

@dlt.table(name='silver_murder_turkey', comment='silver turkey murder data')
def silver_murder_turkey():
    spark.conf.set(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY)
    turkey_df = spark.read\
                    .option("header", "true")\
                    .option("inferSchema", "true")\
                    .csv("abfss://datasets@tfmstorageacc.dfs.core.windows.net/violence_against _women_turkey.csv")

    turkey_df_aggregated = turkey_df.withColumn(
        'year',
        F.year(F.to_date(F.col('Date'), 'dd/MM/yyyy')),
    ).withColumn(
        'country', F.lit('turkiye'),
    ).where(
        F.col('year').isNotNull(),
    ).select(
        F.col('year'),
        F.col('country'),
        F.col('Province').alias('province'),
    ).groupBy('year', 'country', 'province').agg(
        F.count('*').alias('total_cases'),
    )
    turkey_df_all = turkey_df_aggregated.withColumn(
        'Province', F.lit('all'),
    ).select(
        F.col('year'),
        F.col('country'),
        F.col('Province').alias('province'),
        F.col('total_cases'),
    ).groupBy('year', 'country', 'province').agg(
        F.sum('total_cases').alias('total_cases'),
    )
    return turkey_df_aggregated.union(turkey_df_all)