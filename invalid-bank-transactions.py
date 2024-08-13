# Import your libraries
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Start writing code
filter_timestamp=boi_transactions.withColumn('time',date_format(col('time_stamp'),'HH:mm:ss')).filter((col('time') <='09:00:00') | (col('time') >='16:00:00'))
filter_Weekend=boi_transactions.withColumn('Weekday',dayofweek('time_stamp')).filter((col('Weekday')=='1')| (col('Weekday')=='7'))
filter_Holiday=boi_transactions.withColumn('day',dayofmonth('time_stamp')).filter((col('day')==25) | (col('day')==26))

Invalid_Trxs=filter_timestamp.union(filter_Weekend).union(filter_Holiday)
Invalid_trans_id=Invalid_Trxs.select('transaction_id').distinct()

# To validate your solution, convert your final pySpark df to a pandas df
Invalid_trans_id.toPandas()