# Import your libraries
import pyspark
from pyspark.sql.functions import * 

# Start writing code
join_df=signups.join(plans,signups.plan_id==plans.id,'left').withColumn('WeekDay', dayofweek('signup_start_date')-1)
group_df=join_df.groupBy('WeekDay','billing_cycle').agg(count('signup_id').alias('Total_Signup_Id'))
pivot_df=group_df.groupBy('WeekDay').pivot('billing_cycle').sum('Total_Signup_Id')
fill_Zero=pivot_df.fillna(0)
# To validate your solution, convert your final pySpark df to a pandas df
fill_Zero.toPandas()