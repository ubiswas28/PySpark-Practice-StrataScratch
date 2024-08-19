# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
filter_df=fct_customer_sales.withColumn('month',split('order_date','-')[1]).withColumn('quarter',\
        when((col('month') > '00') & (col('month') < '04'),'Q1').\
        when((col('month') > '03') & (col('month') < '07'),'Q2').\
        when((col('month') > '06') & (col('month') < '10'),'Q3').\
        otherwise('Q4')).where((col('quarter')=='Q3')|(col('quarter')=='Q4'))
rename_df=filter_df.withColumnRenamed('cust_id','id')
join_df=rename_df.join(map_customer_territory, map_customer_territory.cust_id==rename_df.id,'right')
month_df=join_df.select('territory_id','order_value','quarter')
group_df=month_df.groupBy('territory_id').pivot('quarter').agg(sum('order_value').alias('Total_Order')).\
		 orderBy('territory_id').fillna(0)
sales_growth=group_df.withColumn('Sales Growth', round(((col('Q4')-col('Q3'))/col('Q3'))*100,2)).fillna(0)\
            .select('territory_id','Sales Growth')    
# To validate your solution, convert your final pySpark df to a pandas df
sales_growth.toPandas()