# run this file in databricks, Update mode works with csv format also.

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

# for explode, & flatten its kept outside the function, because no aggregation is inolved.
# if no aggrgegation in between readstream to the write stream then no meteo store will be created 
# under the spark UI -> structured streaming tab.

exploded_orders.createOrReplaceTempView("exploded_orders")

flattened_orders = spark.sql("""select order_id, customer_id, city, state, pincode, 
lines.order_item_id as item_id, lines.order_item_product_id as product_id,
lines.order_item_quantity as quantity,lines.order_item_product_price as price,
lines.order_item_subtotal as subtotal from exploded_orders""")

def myfunction(flattened_orders,batch_id):

    flattened_orders.createOrReplaceTempView("orders_flattened")

    aggregated_orders = flattened_orders._jdf.sparkSession().sql("""select customer_id, count(distinct(order_id)) as orders_placed, 
    count(item_id) as products_purchased,sum(subtotal) as amount_spent 
    from orders_flattened group by customer_id""")


    aggregated_orders.createOrReplaceTempView("orders_result")
    merge_statement = """merge into orders_final_result t using orders_result s
    on t.customer_id == s.customer_id
    when matched then
    update set t.products_purchased = t.products_purchased + s.products_purchased, 
    t.orders_placed = t.orders_placed + s.orders_placed,
    t.amount_spent = t.amount_spent + s.amount_spent
    when not matched then
    insert *
    """

    flattened_orders._jdf.sparkSession().sql(merge_statement)
# before the write stream, we've not done any aggregations, this is still flattened_orders
# we've implemented the aggregation under for each batch functions.
# that's where state store will not come into play. 
# this is the case of unbounded live streaming data
# for bounded we can go with the state approach methods. 
streaming_query = flattened_orders \
.writeStream \
.format("delta") \
.outputMode("update") \
.option("checkpointLocation","checkpointdir108") \
.foreachBatch(myfunction) \
.start()
# totable will work with append, & complete mode.
# it won't work with update mode. whenever you have to implement custom logic, use below fn.

spark.sql("create table orders_final_result (customer_id long, orders_placed long, products_purchased long, amount_spent float)")

spark.sql("select * from orders_final_result").show()