# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
rename_col=transactions.withColumnRenamed('signup_id','SignID')
join_trxs=signups.join(rename_col,signups.signup_id==rename_col.SignID, 'left')
filter_df=join_trxs.filter(months_between((lit('2021-03-01').cast('date')),col('transaction_start_date')) >=10)
join_plans=filter_df.join(plans,join_trxs.plan_id==plans.id,'left').groupBy('signup_id','billing_cycle').agg(avg('amt').alias('Avg_Amt')).orderBy(desc('billing_cycle'),'signup_id')
# To validate your solution, convert your final pySpark df to a pandas df
join_plans.toPandas()