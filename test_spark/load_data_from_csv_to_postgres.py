import pyspark
import pyspark.sql.types as type_spark
from pyspark.sql.functions import desc
# Создаем SparkSession
spark = pyspark.sql.SparkSession.builder.appName("PopularProductPerCustomer").getOrCreate()
customer_schema = type_spark.StructType() \
      .add("id",type_spark.IntegerType(),True) \
      .add("name",type_spark.StringType(),True) \
      .add("email",type_spark.StringType(),True) \
      .add("joinDate",type_spark.DateType(),True) \
      .add("status",type_spark.StringType(),True) 
product_schema = type_spark.StructType() \
      .add("id",type_spark.IntegerType(),True) \
      .add("name",type_spark.StringType(),True) \
      .add("price",type_spark.DoubleType(),True) \
      .add("numberOfProducts",type_spark.DateType(),True) 
order_schema = type_spark.StructType() \
      .add("customerID",type_spark.IntegerType(),True) \
      .add("orderID",type_spark.IntegerType(),True) \
      .add("productID",type_spark.IntegerType(),True) \
      .add("numberOfProduct",type_spark.IntegerType(),True) \
      .add("orderDate",type_spark.DateType(),True) \
      .add("status",type_spark.StringType(),True)       
# Читаем данные из CSV-файлов
customer_df = spark.read.options(delimiter='\t').schema(customer_schema).csv("Customer.csv")
product_df = spark.read.options(delimiter='\t').schema(product_schema).csv("Product.csv")
order_df = spark.read.options(delimiter='\t').schema(order_schema).csv("Order.csv")
# Объединяем таблицы
order_product_df = order_df.join(product_df, order_df.productID == product_df.id).select("customerID", "name", "numberOfProduct").withColumnRenamed("name","productName")
#order_product_df.show()
# Группируем по customerID и находим самый популярный продукт
popular_product_df = order_product_df.groupBy("customerID", "productName").sum("numberOfProduct").orderBy("customerID",desc("sum(numberOfProduct)") ).groupBy("customerID").agg({"productName": "first"}).withColumnRenamed("first(productName)", "popularProductName")
#popular_product_df.show()
popular_product_df =customer_df.join(popular_product_df, popular_product_df.customerID == customer_df.id,"left") \
                                     .select("name", "popularProductName").withColumnRenamed("name", "CustomerName")
#popular_product_df.show()
popular_product_df.write.format("csv").mode('overwrite').save("result")
# Останавливаем SparkSession
spark.stop()