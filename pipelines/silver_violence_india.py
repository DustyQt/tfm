import pyspark.sql.functions as F
import os


@dlt.table(name='silver_violence_india', comment='silver violence in india data')
def silver_violence_india():
    india_df = spark.table('hive_metastore.azure_sql_db_dbo.crimes_on_women')
    india_df_renamed_melted = india_df.where(
    F.col('_fivetran_deleted') == False,
    ).withColumn(
        'country', F.lit('india'),
    ).select(
        F.col('country'),
        F.col('Year').alias('year'),
        F.col('State').alias('state'),
        F.col('Rape').alias('rape'),
        F.col('ka').alias('kidnap_assault'),
        F.col('dd').alias('dowry_deaths'),
        F.col('dv').alias('domestic_violence'),
        F.col('aow').alias('assault'),
        F.col('aom').alias('assault_on_modesty'),
    ).melt(
        ids=['country', 'year', 'state'],
        values=None,
        valueColumnName='total_cases',
        variableColumnName='type_of_violence',
    )
    india_df_any_violence = india_df_renamed_melted.groupBy('year', 'country', 'state', 'type_of_violence').agg(
        F.sum('total_cases').alias('total_cases')
    ).withColumn(
        'type_of_violence', F.lit('any')
    ).select(
        F.col('country'),
        F.col('year'),
        F.col('state'),
        F.col('type_of_violence'),
        F.col('total_cases')
    )
    india_df_any_violence = india_df_renamed_melted.union(india_df_any_violence)
    india_df_total = india_df_any_violence.groupBy('year', 'country', 'type_of_violence').agg(
        F.sum('total_cases').alias('total_cases')
    ).withColumn(
        'state', F.lit('all')
    ).select(
        F.col('country'),
        F.col('year'),
        F.col('state'),
        F.col('type_of_violence'),
        F.col('total_cases')
    )
    return india_df_total.union(india_df_any_violence)
    