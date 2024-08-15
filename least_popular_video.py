# Import your libraries
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.window import *

# Start writing code
window_spec=Window.orderBy('Total_User')
Least_popular=videos_watched.groupBy('video_id').agg((count_distinct('user_id')).alias("Total_User")).withColumn('rank',dense_rank().over(window_spec)).where(col('rank')==1).select(col('video_id'))

# To validate your solution, convert your final pySpark df to a pandas df
Least_popular.toPandas()