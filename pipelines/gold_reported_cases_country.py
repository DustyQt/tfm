import pyspark.sql.functions as F
import os


@dlt.table(name='gold_reported_cases_country', comment='Gold reported cases by country')
def gold_reported_cases_country():
    country_df = dlt.read('silver_country')
    colombia_df = dlt.read('silver_violence_colombia')
    india_df = dlt.read('silver_violence_india')
    bangladesh_df = dlt.read('silver_rape_bangladesh')
    malaysia_df = dlt.read('silver_violence_malaysia')
    turkey_df = dlt.read('silver_murder_turkey')

    colombia_df_filtered = colombia_df.where(
        (F.col('department') == 'all') &
        (F.col('age_group') == 'any') &
        (F.col('perpetrator') == 'any')
    ).alias('colombia_df').join(
        country_df.alias('country_df'),
        (country_df.country_name == colombia_df.country) & (country_df.year == colombia_df.year),
    ).select(
        F.col('country_df.country_name'),
        F.col('country_df.country_code'),
        F.col('country_df.year'),
        F.col('colombia_df.total_cases'),
        F.col('country_df.female_population')
    )

    india_df_filtered = india_df.where(
        (F.col('state') == 'all') &
        (F.col('type_of_violence') == 'any')
    ).alias('india_df').join(
        country_df.alias('country_df'),
        (F.lower(country_df.country_name) == F.lower(india_df.country)) & (country_df.year == india_df.year)
    ).select(
        F.col('country_df.country_name'),
        F.col('country_df.country_code'),
        F.col('country_df.year'), 
        F.col('india_df.total_cases'),
        F.col('country_df.female_population')
    )

    bangladesh_df_filtered = bangladesh_df.where(
        (F.col('age_group') == 'any')
    ).alias('bangladesh_df').join(
        country_df.alias('country_df'),
        (F.lower(country_df.country_name) == F.lower(bangladesh_df.country)) & (country_df.year == bangladesh_df.year)
    ).select(
        F.col('country_df.country_name'),
        F.col('country_df.country_code'),
        F.col('country_df.year'), 
        F.col('bangladesh_df.total_cases'),
        F.col('country_df.female_population')
    )

    malaysia_df_filtered = malaysia_df.where(
        (F.col('type_of_violence') == 'any')
    ).alias('malaysia_df').join(
        country_df.alias('country_df'),
        (F.lower(country_df.country_name) == F.lower(malaysia_df.country)) & (country_df.year == malaysia_df.year)
    ).select(
        F.col('country_df.country_name'),
        F.col('country_df.country_code'),
        F.col('country_df.year'), 
        F.col('malaysia_df.total_cases'),
        F.col('country_df.female_population')
    )

    turkey_df_filtered = turkey_df.where(
        (F.col('province') == 'all')
    ).alias('turkey_df').join(
        country_df.alias('country_df'),
        (F.lower(country_df.country_name) == F.lower(turkey_df.country)) & (country_df.year == turkey_df.year)
    ).select(
        F.col('country_df.country_name'),
        F.col('country_df.country_code'),
        F.col('country_df.year'), 
        F.col('turkey_df.total_cases'),
        F.col('country_df.female_population')
    )
    
    return turkey_df_filtered.union(malaysia_df_filtered).union(bangladesh_df_filtered).union(india_df_filtered).union(colombia_df_filtered)