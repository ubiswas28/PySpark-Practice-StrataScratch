# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
filter_friday=user_purchases.where(col('day_name')=='Friday').withColumn('Week_Num',weekofyear('date')).groupBy('Week_Num','user_id').agg(avg('amount_spent').alias('Average_Amt')).orderBy('Week_Num').select('Week_Num','Average_Amt')

# To validate your solution, convert your final pySpark df to a pandas df
filter_friday.toPandas()