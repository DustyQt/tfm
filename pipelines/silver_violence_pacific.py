import pyspark.sql.functions as F
import os

STORAGE_ACCOUNT = os.getenv('STORAGE_ACCOUNT')
STORAGE_ACCOUNT_KEY = os.getenv('STORAGE_ACCOUNT_KEY')

@dlt.table(name='silver_pacific_violence', comment='Silver Pacific violence data')
def silver_pacific_violence():
    spark.conf.set(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY)
    pacific_df = spark.read\
                    .option("header", "true")\
                    .option("inferSchema", "true")\
                    .csv("abfss://datasets@tfmstorageacc.dfs.core.windows.net/violence_against_women_pacific.csv")

    pacific_df_filtered = pacific_df.where(
        F.col('TOPIC10').isin('VAW_TOPIC_001', 'VAW_TOPIC_007', 'VAW_TOPIC_010') &
        F.col('OBS_VALUE').isNotNull()
    ).select(
        F.col('TIME_PERIOD').alias('year'),
        F.col('Pacific Island Countries and territories').alias('country'),
        F.col('Type of violence').alias('violence_type'),
        F.col('OBS_VALUE').alias('value_perc'),
        F.col('Perpetrator23').alias('perpetrator'),
    ).groupBy('year', 'country', 'violence_type', 'perpetrator').agg(
        F.max(F.col('value_perc')).alias('value_perc')
    )
    return pacific_df_filtered