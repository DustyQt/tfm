import pyspark.sql.functions as F
import os


@dlt.table(name='country', comment='Country data')
def silver_country():
    country_df = spark.table('hive_metastore.dynamodb.population_female')
    return country_df.filter(
        F.col('_fivetran_deleted') == False
    ).drop(
        '_fivetran_synced',
    ).drop(
        'indicator_code',
    ).drop(
        'indicator_name',
    ).drop(
        '_fivetran_deleted',
    ).melt(
        ids=['country_name', 'country_code' ],
        values=None,
        variableColumnName="year",
        valueColumnName="female_population"
    ).withColumn('year', F.col('year').substr(2,5))