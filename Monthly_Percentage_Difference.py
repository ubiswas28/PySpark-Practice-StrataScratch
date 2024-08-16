# Import your libraries
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.window import *

# Start writing code
year_month_df=sf_transactions.withColumn('Year_Month',concat_ws('-', split('created_at','-')[0],\
              split('created_at','-')[1])).groupBy('Year_Month').agg(sum('value').alias('Total_Monthly_Revenue'))
window_spec=Window.orderBy('Year_Month')
Prev_Rev_df=year_month_df.withColumn('Prev_Rev',lag('Total_Monthly_Revenue').over(window_spec))
Percent_change_df=Prev_Rev_df.withColumn('Percent_Change',round((col('Prev_Rev')-col('Total_Monthly_Revenue')) \
                  *100/col('Total_Monthly_Revenue'),2)).fillna(0)
# To validate your solution, convert your final pySpark df to a pandas df
Percent_change_df.toPandas()