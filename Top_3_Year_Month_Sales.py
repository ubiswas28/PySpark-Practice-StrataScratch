# Import your libraries
import pyspark
from pyspark.sql.functions import *
# Start writing code
final_df=fct_customer_sales.withColumn('date',date_format('order_date','yyyy-MM')).groupBy('date').agg(sum('order_value').alias('Monthly_Sales')).orderBy(desc('Monthly_Sales')).limit(3)

# To validate your solution, convert your final pySpark df to a pandas df
final_df.toPandas()