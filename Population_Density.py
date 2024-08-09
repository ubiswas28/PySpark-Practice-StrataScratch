# Import your libraries
import pyspark
from pyspark.sql.functions import * 

#removing the records that has 'area' as '0.0'

filter_df=cities_population.where(col('area')!= '0.0' )

# Start writing code
density_df=filter_df.withColumn('Density',round(col('population')/col('area')))
max_df=density_df.orderBy(col('Density').desc()).limit(1)
min_df=density_df.orderBy(col('Density').asc()).limit(1)

final_df=max_df.union(min_df).select('city','country','density')

# To validate your solution, convert your final pySpark df to a pandas df
final_df.toPandas()
