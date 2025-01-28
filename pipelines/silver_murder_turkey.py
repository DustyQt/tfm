import pyspark.sql.functions as F
import os

STORAGE_ACCOUNT = os.getenv('STORAGE_ACCOUNT')
STORAGE_ACCOUNT_KEY = os.getenv('STORAGE_ACCOUNT_KEY')

@dlt.table(name='silver_turkey_murder', comment='silver turkey murder data')
def silver_turkey_murder():
    spark.conf.set(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY)
    turkey_df = spark.read\
                    .option("header", "true")\
                    .option("inferSchema", "true")\
                    .csv("abfss://datasets@tfmstorageacc.dfs.core.windows.net/violence_against _women_turkey.csv")

    return turkey_df.withColumn(
        'year',
        F.year(F.to_date(F.col('Date'), 'dd/MM/yyyy')),
    ).withColumn(
        'country', F.lit('turkey'),
    ).withColumn(
        'age_group', F.lit('any'),
    ).where(
        F.col('year').isNotNull(),
    ).select(
        F.col('age_group'),
        F.col('year'),
        F.col('country'),
        F.col('Province').alias('province'),
    ).groupBy('age_group', 'year', 'country', 'province').agg(
        F.count('*').alias('total_cases'),
    )