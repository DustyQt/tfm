import pyspark.sql.functions as F
import os

STORAGE_ACCOUNT = os.getenv('STORAGE_ACCOUNT')
STORAGE_ACCOUNT_KEY = os.getenv('STORAGE_ACCOUNT_KEY')

@dlt.table(name='silver_perception_global', comment='silver global perception on situations that legitimaze woman violence')
def silver_perception_global():
    spark.conf.set(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY)
    df = spark.read\
                 .option("header", "true")\
                 .option("inferSchema", "true")\
                 .csv("abfss://datasets@tfmstorageacc.dfs.core.windows.net/violence_data_sentiment.csv")
    return df.withColumn(
        'year',
        F.year(F.to_date(F.col('Survey Year'), 'dd/MM/yyyy')),
    ).where(
        F.col('Value').isNotNull()
    ).select(
        F.col('Country').alias('country'),
        F.col('year'),
        F.col('Gender').alias('gender'),
        F.col('Demographics Question').alias('demographics_question'),
        F.col('Demographics Response').alias('demographics_response'),
        F.col('Question').alias('question'),
        F.col('Value').alias('value_perc'),
    )