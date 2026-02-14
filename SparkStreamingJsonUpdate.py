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

spark.sql("select * from orders Limit 10").show(10)
# +--------+-----------+--------------+--------------+-------+-----+-------+--------------------+
# |order_id|customer_id|customer_fname|customer_lname|   city|state|pincode|          line_items|
# +--------+-----------+--------------+--------------+-------+-----+-------+--------------------+
# |       1|      11599|          Mary|        Malone|Hickory|   NC|  28601|[{1, 957, 1, 299....|
# |       2|        256|         David|     Rodriguez|Chicago|   IL|  60625|[{2, 1073, 1, 199...|
# +--------+-----------+--------------+--------------+-------+-----+-------+--------------------+

exploded_orders = spark.sql("""select order_id,customer_id,city,state,
pincode,explode(line_items) lines from orders""")

spark.sql(""" SELECT order_id,
    customer_id,
    customer_fname,
    customer_lname,
    city,
    state,
    pincode,
    explode(line_items) lines from orders""").show(10, truncate= False)
# +--------+-----------+--------------+--------------+-------+-----+-------+----------------------------+
# |order_id|customer_id|customer_fname|customer_lname|city   |state|pincode|lines                       |
# +--------+-----------+--------------+--------------+-------+-----+-------+----------------------------+
# |1       |11599      |Mary          |Malone        |Hickory|NC   |28601  |{1, 957, 1, 299.98, 299.98} |
# |2       |256        |David         |Rodriguez     |Chicago|IL   |60625  |{2, 1073, 1, 199.99, 199.99}|
# |2       |256        |David         |Rodriguez     |Chicago|IL   |60625  |{3, 502, 5, 50.0, 250.0}    |
# |2       |256        |David         |Rodriguez     |Chicago|IL   |60625  |{4, 403, 1, 129.99, 129.99} |
# +--------+-----------+--------------+--------------+-------+-----+-------+----------------------------+

spark.sql(""" SELECT order_id,
    customer_id,
    customer_fname,
    customer_lname,
    city,
    state,
    pincode,
    item.order_item_id,
    item.order_item_product_id,
    item.order_item_quantity,
    item.order_item_subtotal,
    item.order_item_product_price
FROM orders
LATERAL VIEW EXPLODE(line_items) AS item""").show(10, truncate= False)

# +--------+-----------+--------------+--------------+-------+-----+-------+-------------+---------------------+-------------------+-------------------+------------------------+
# |order_id|customer_id|customer_fname|customer_lname|city   |state|pincode|order_item_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
# +--------+-----------+--------------+--------------+-------+-----+-------+-------------+---------------------+-------------------+-------------------+------------------------+
# |1       |11599      |Mary          |Malone        |Hickory|NC   |28601  |1            |957                  |1                  |299.98             |299.98                  |
# |2       |256        |David         |Rodriguez     |Chicago|IL   |60625  |2            |1073                 |1                  |199.99             |199.99                  |
# |2       |256        |David         |Rodriguez     |Chicago|IL   |60625  |3            |502                  |5                  |250.0              |50.0                    |
# |2       |256        |David         |Rodriguez     |Chicago|IL   |60625  |4            |403                  |1                  |129.99             |129.99                  |
# +--------+-----------+--------------+--------------+-------+-----+-------+-------------+---------------------+-------------------+-------------------+------------------------+

exploded_orders.createOrReplaceTempView("exploded_orders")

flattened_orders = spark.sql("""select order_id, customer_id, city, state, pincode, 
lines.order_item_id as item_id, lines.order_item_product_id as product_id,
lines.order_item_quantity as quantity,lines.order_item_product_price as price,
lines.order_item_subtotal as subtotal from exploded_orders""")

flattened_orders.createOrReplaceTempView("orders_flattened")

aggregated_orders = spark.sql("""select customer_id, count(distinct(order_id)) as orders_placed, 
count(item_id) as products_purchased,sum(subtotal) as amount_spent 
from orders_flattened group by customer_id""")

def myfunction(orders_result,batch_id):
    orders_result.createOrReplaceTempView("orders_result")
    merge_statement = """merge into orders_final_result t using orders_result s
    on t.customer_id == s.customer_id
    when matched then
    update set t.products_purchased = s.products_purchased, t.orders_placed = s.orders_placed,
    t.amount_spent = s.amount_spent
    when not matched then
    insert *
    """

    orders_result._jdf.sparkSession().sql(merge_statement)

streaming_query = aggregated_orders \
.writeStream \
.format("csv") \
.outputMode("update") \
.option("checkpointLocation","checkpointdir108") \
.foreachBatch(myfunction) \
.start()
# totable will work with append, & complete mode.
# it won't work with update mode. whenever you have to implement custom logic, use below fn.

spark.sql("create table orders_final_result (customer_id long, orders_placed long, products_purchased long, amount_spent float)")

spark.sql("select * from orders_final_result").show()
