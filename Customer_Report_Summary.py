# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
#df=wfm_transactions.withColumn('Month',month(col('transaction_date'))).where(col('Month')==1)
Cust_Sum_Rep=wfm_transactions.where(col('sales') >= '5').withColumn('Month',month(col('transaction_date'))).groupBy('Month').agg(countDistinct(('customer_id')).alias("Total_Customer"),sum('sales').alias('Total_Transaction')).orderBy('Month')

# To validate your solution, convert your final pySpark df to a pandas df
Cust_Sum_Rep.toPandas()