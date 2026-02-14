from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import getpass
username = getpass.getuser()
print(username)
spark = SparkSession \
    .builder \
    .config('spark.sql.shuffle.partitions', 3) \
    .master("local[2]") \
    .getOrCreate()

# use ArrayType instead of StructType for line_items
{"order_id":1,"customer_id":11599,"customer_fname":"Mary","customer_lname":"Malone","city":"Hickory","state":"NC","pincode":28601,"line_items":[{"order_item_id":1,"order_item_product_id":957,"order_item_quantity":1,"order_item_subtotal":299.98,"order_item_product_price":299.98}]}

orders_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("customer_fname", StringType(), True),
    StructField("customer_lname", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("pincode", LongType(), True),
    StructField("line_items", (StructType([
        StructField("order_item_id", LongType(), True),
        StructField("order_item_product_id", LongType(), True),
        StructField("order_item_quantity", LongType(), True),
        StructField("order_item_product_price", FloatType(), True),
        StructField("order_item_subtotal", FloatType(), True)
    ])), True)
])

orders_df = spark \
.read \
.format("json") \
.schema(orders_schema) \
.option("path","/Users/gauravmishra/Desktop/test_folder") \
.load()

orders_df.createOrReplaceTempView("orders")

spark.sql("select * from orders Limit 10").show(10, truncate= False)
# --------+-----------+--------------+--------------+-------+-----+-------+----------+
# |order_id|customer_id|customer_fname|customer_lname|city   |state|pincode|line_items|
# +--------+-----------+--------------+--------------+-------+-----+-------+----------+
# |1       |11599      |Mary          |Malone        |Hickory|NC   |28601  |NULL      |
# |2       |256        |David         |Rodriguez     |Chicago|IL   |60625  |NULL      |
# +--------+-----------+--------------+--------------+-------+-----+-------+----------+

orders_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("customer_fname", StringType(), True),
    StructField("customer_lname", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("pincode", LongType(), True),
    StructField("line_items", ArrayType(StructType([
        StructField("order_item_id", LongType(), True),
        StructField("order_item_product_id", LongType(), True),
        StructField("order_item_quantity", LongType(), True),
        StructField("order_item_product_price", FloatType(), True),
        StructField("order_item_subtotal", FloatType(), True)
    ])), True)
])

orders_df = spark \
.read \
.format("json") \
.schema(orders_schema) \
.option("path","/Users/gauravmishra/Desktop/test_folder") \
.load()

orders_df.createOrReplaceTempView("orders")

spark.sql("select * from orders Limit 10").show(10, truncate= False)
# +--------+-----------+--------------+--------------+-------+-----+-------+-------------------------------------------------------------------------------------+
# |order_id|customer_id|customer_fname|customer_lname|city   |state|pincode|line_items                                                                           |
# +--------+-----------+--------------+--------------+-------+-----+-------+-------------------------------------------------------------------------------------+
# |1       |11599      |Mary          |Malone        |Hickory|NC   |28601  |[{1, 957, 1, 299.98, 299.98}]                                                        |
# |2       |256        |David         |Rodriguez     |Chicago|IL   |60625  |[{2, 1073, 1, 199.99, 199.99}, {3, 502, 5, 50.0, 250.0}, {4, 403, 1, 129.99, 129.99}]|
# +--------+-----------+--------------+--------------+-------+-----+-------+-------------------------------------------------------------------------------------+

