# Import your libraries
import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Start writing code
emp_joined=lyft_drivers.withColumn('start_yr',regexp_extract('start_date',r'(\d{4})-(\d{2})-(\d{2})',1)).withColumn('end_yr',regexp_extract('end_date',r'(\d{4})-(\d{2})-(\d{2})',1)).agg(count('start_yr').alias('Total_Emp_Joined'),count('end_yr').alias('Total_Emp_left'))
churn_rate=emp_joined.withColumn('Churn_rate',((col('Total_Emp_left')/col('Total_Emp_Joined'))*100))
# To validate your solution, convert your final pySpark df to a pandas df
churn_rate.toPandas()
