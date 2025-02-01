import pyspark.sql.functions as F
import os

STORAGE_ACCOUNT = os.getenv('STORAGE_ACCOUNT')
STORAGE_ACCOUNT_KEY = os.getenv('STORAGE_ACCOUNT_KEY')

@dlt.table(name='silver_rape_bangladesh', comment='silver_rape_bangladesh data')
def silver_rape_bangladesh():
    spark.conf.set(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY)
    bangladesh_df = spark.read\
                    .option("header", "true")\
                    .option("inferSchema", "true")\
                    .csv("abfss://datasets@tfmstorageacc.dfs.core.windows.net/rape_total_2001_2021-bangladesh.csv")
    age_group_dict = {
        'children':'child',
        'women':'adult',
        'Total_victims':'any'
    }
    return bangladesh_df.drop(
        'Unidentified_age'
    ).melt(
        ids=['Years'],
        values=['Total_victims', 'children', 'women'],
        variableColumnName='age_group',
        valueColumnName='total_cases',
    ).replace(
        age_group_dict,
        subset=['age_group'],
    ).withColumn(
        'country', F.lit('bangladesh'),
    ).select(
        F.col('Years').alias('year'),
        F.col('age_group'),
        F.col('total_cases'),
        F.col('country'),
    )