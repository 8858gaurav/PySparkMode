# run this file in databricks, complete mode works with delta format only

# update mode will not work, when we run this code on our local mac machine
# pyspark.errors.exceptions.captured.AnalysisException: [STREAMING_OUTPUT_MODE.UNSUPPORTED_DATASOURCE] Invalid streaming output mode: update. This output mode is not supported in Data Source csv. SQLSTATE: 42KDE
# when we run with complet mode, this will not work
# pyspark.errors.exceptions.captured.AnalysisException: [STREAMING_OUTPUT_MODE.UNSUPPORTED_DATASOURCE] Invalid streaming output mode: complete. This output mode is not supported in Data Source csv. SQLSTATE: 42KDE
# json/parquet will also not work, if we use update mode on our local mac machine
# pyspark.errors.exceptions.captured.AnalysisException: [STREAMING_OUTPUT_MODE.UNSUPPORTED_DATASOURCE] Invalid streaming output mode: update. This output mode is not supported in Data Source parquet. SQLSTATE: 42KDE
# pyspark.errors.exceptions.captured.AnalysisException: [STREAMING_OUTPUT_MODE.UNSUPPORTED_DATASOURCE] Invalid streaming output mode: update. This output mode is not supported in Data Source json. SQLSTATE: 42KDE


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import getpass
username = getpass.getuser()
print(username)
spark = SparkSession \
    .builder \
    .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
    .enableHiveSupport() \
    .master("local[2]") \
    .getOrCreate()

orders_schema = "order_id long,customer_id long,customer_fname string,customer_lname string,city string,state string,pincode long,line_items array<struct<order_item_id: long,order_item_product_id: long,order_item_quantity: long,order_item_product_price: float,order_item_subtotal: float>>"

orders_df = spark \
.readStream \
.format("json") \
.schema(orders_schema) \
.option("path","/Users/gauravmishra/Desktop/Mode/Input_dir") \
.load()

orders_df.createOrReplaceTempView("orders")

exploded_orders = spark.sql("""select order_id,customer_id,city,state,
pincode,explode(line_items) lines from orders""")

exploded_orders.createOrReplaceTempView("exploded_orders")

flattened_orders = spark.sql("""select order_id, customer_id, city, state, pincode, 
lines.order_item_id as item_id, lines.order_item_product_id as product_id,
lines.order_item_quantity as quantity,lines.order_item_product_price as price,
lines.order_item_subtotal as subtotal from exploded_orders""")

flattened_orders.createOrReplaceTempView("orders_flattened")

aggregated_orders = spark.sql("""select customer_id, approx_count_distinct(order_id) as orders_placed, 
count(item_id) as products_purchased,sum(subtotal) as amount_spent 
from orders_flattened group by customer_id""")

streaming_query = aggregated_orders \
.writeStream \
.format("delta") \
.outputMode("complete") \
.option("checkpointLocation","checkpointdir1") \
.toTable("orders_result1")
# totable will work with append, & complete mode.
# it won't work with update mode.

spark.sql("select * from orders_result1").show()
