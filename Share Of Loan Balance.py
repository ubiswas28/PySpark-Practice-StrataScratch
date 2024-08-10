# Import your libraries
import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
window_spec=Window.partitionBy('rate_type')
# Start writing code
submissions=submissions.withColumn("balance",col("balance").cast("double"))
total_balance_df=submissions.withColumn("Total_Balance",sum('balance').over(window_spec)).withColumn("Percentage_Loan_Balance", concat(round((col('balance')/col('Total_Balance')*100),2),lit('%'))).select('rate_type','loan_id','balance','Percentage_Loan_Balance')

# To validate your solution, convert your final pySpark df to a pandas df
total_balance_df.toPandas()