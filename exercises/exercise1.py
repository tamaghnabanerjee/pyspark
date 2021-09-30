#What is the average revenue of the orders?

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

#create SparkSession object
spark = SparkSession.builder.appName("exercise1").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#read source files into tables
orders = spark.read.format("parquet")\
                    .options(header=True)\
                    .load("file:///home/tamaghna/big_data_spark/sales_parquet")

products = spark.read.format("parquet")\
                        .options(header=True)\
                        .load("file:///home/tamaghna/big_data_spark/products_parquet")
						
sellers = spark.read.format("parquet")\
                    .options(header=True)\
                    .load("file:///home/tamaghna/big_data_spark/sellers_parquet")

#write the logic
orders_revenue = orders.join(products,orders.product_id==products.product_id).agg(avg(products.price*orders.num_pieces_sold))
print(orders_revenue.show())
