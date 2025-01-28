import pyspark.sql.functions as F
import os


@dlt.table(name='silver_violence_malaysia', comment='silver violence in malaysia data')
def silver_violence_malaysia():
    malaysia_df = spark.table('hive_metastore.google_drive._2021_violence_against_women_cases_malasia')
    malaysia_df_filtered = malaysia_df.where(
        F.col('sex') == 'Women',
    ).withColumn(
        'country', F.lit('malaysia'),
    ).select(
        F.col('country'),
        F.col('year'),
        F.col('type_of_case').alias('type_of_violence'),
        F.col('number_of_violence').alias('total_cases'),
    )

    malaysa_df_any = malaysia_df_filtered.groupBy('year', 'country').agg(
        F.sum('total_cases').alias('total_cases')
    ).withColumn(
        'type_of_violence', F.lit('any')
    ).select(
        F.col('year'),
        F.col('country'),
        F.col('type_of_violence'),
        F.col('total_cases')
    )
    return malaysia_df_any_violence = malaysia_df_filtered.union(malaysa_df_any)
    