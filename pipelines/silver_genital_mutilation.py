import pyspark.sql.functions as F
import os


@dlt.table(name='silver_genital_mutilation', comment='silver genital mutilation mainly in africa')
def silver_genital_mutilation():
    gm_df = spark.table('hive_metastore.google_drive.xls_fgm_women_prevalence_database_mar_2024_women_fgm')
    return gm_df.withColumn(
        'country', F.lower(F.col('country')),
    ).where(
        F.col('total_perc') != '-',
    ).select(
        F.col('country'),
        F.col('urban_perc'),
        F.col('rural_perc'),
        F.col('richest_perc'),
        F.col('poores_perc').alias('poorest_perc'),
        F.col('total_perc'),
    )
    