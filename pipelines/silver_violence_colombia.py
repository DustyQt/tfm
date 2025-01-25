import pyspark.sql.functions as F
import os

STORAGE_ACCOUNT = os.getenv('STORAGE_ACCOUNT')
STORAGE_ACCOUNT_KEY = os.getenv('STORAGE_ACCOUNT_KEY')

@dlt.table(name='colombia_violence', comment='colombia violence data')
def silver_colombia_violence():
    spark.conf.set(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY)
    colombian_df = spark.read\
                    .option("header", "true")\
                    .option("inferSchema", "true")\
                    .csv("abfss://datasets@tfmstorageacc.dfs.core.windows.net/violencia_intrafamiliar_colombia.csv")

    colombian_df_filtered = colombian_df.where(
        F.col('Sexo de la victima') == 'Mujer'
    ).withColumn(
        'age_group',
        F.when(F.col('Grupo Mayor Menor de Edad') == 'b) Mayores de Edad (>18 años)', 'adult').otherwise('child')
    ).withColumn(
        'country',
        F.lit('Colombia'),
    ).withColumn(
        'perpetrator',
        F.when(F.col('Presunto Agresor').isin(
            'Amante',
            'Compañero (a) permanente',
            'Esposo (a)',
            'Pareja o Expreja',
            'Novio (a)',
        ), 'partner').otherwise('non-partner')
    ).select(
        F.col('country'),
        F.col('Departamento del hecho DANE').alias('department'),
        F.col('Año del hecho').alias('year'),
        F.col('perpetrator'),
        F.col('age_group'),
    ).cache()

    colombian_df_perpetrator = colombian_df_filtered.groupBy('year', 'country', 'department', 'perpetrator', 'age_group').agg(
        F.count('*').alias('total_cases'),
    )
    # for the union we need to make the columns match
    colombian_df_any = colombian_df_filtered.groupBy('year', 'country', 'department', 'age_group').agg(
        F.count('*').alias('total_cases')
    ).withColumn(
        'perpetrator', F.lit('any')
    ).select(
        F.col('year'),
        F.col('country'),
        F.col('department'),
        F.col('perpetrator'),
        F.col('age_group'),
        F.col('total_cases'),
    )

    return colombian_df_any.union(colombian_df_perpetrator)