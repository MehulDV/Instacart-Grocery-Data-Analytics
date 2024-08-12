package org.bigdata.grocery.dataframe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.bigdata.grocery.utils.Constants._
import org.bigdata.grocery.schema.InstaCartDbSchema

object Aggregation extends InstaCartDbSchema with App {

  System.setProperty("hadoop.home.dir", "C:\\Hadoop\\")

  val spark: SparkSession = SparkSession.builder().master("local[1]")
    .appName("Grocery Data Analysis")
    .getOrCreate()

//  spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")

  val productsDF = spark.read.parquet(productsFilePath)
  val ordersDF = spark.read.parquet(orders_FilePath)
  val aislesDF = spark.read.parquet(aislesFilePath)
  val departmentDF = spark.read.parquet(departmentsFilePath)
  val order_products_priorDF = spark.read.parquet(order_products_priorFilePath)
  val order_products_trainDF = spark.read.parquet(order_products_trainFilePath)

  // Filtering data using Where & Filter
  val productNamesDFUsingWhere = productsDF
    .where(col("aisle_id") === 1)
    .select(col("product_name"))

  val productNamesDFUsingFilter = productsDF
    .filter(col("aisle_id") === 1)
    .select("product_name")

  val orderCountDF = ordersDF
    .groupBy(col("user_id"))
    .count()
    .withColumnRenamed("count", "order_count")
  orderCountDF.show()

  val order_dow_CountDF = ordersDF
    .groupBy(col("order_dow"))
    .count()
    .withColumnRenamed("count", "order_dow_count")
  order_dow_CountDF.show()

  val cartOrderDF = order_products_priorDF
    .filter(col("order_id") === 10)
    .select(col("product_id"), col("add_to_cart_order").cast("int").as("add_to_cart_order"))
    .orderBy(asc("add_to_cart_order"))
  cartOrderDF.show()

  val productCartOrderDF = order_products_priorDF
    .filter(col("order_id") === 10)
    .select("product_id", "add_to_cart_order")
    .orderBy("add_to_cart_order")
  productCartOrderDF.show()

  // Find the average number of products per order
  val productCountDF = order_products_priorDF
    .groupBy("order_id")
    .count()
    .withColumnRenamed("count", "product_count")

  val averageProductCount = productCountDF
    .agg(avg("product_count").alias("avg_product_count"))
  averageProductCount.show()


  // Most popular time of day for placing orders
  val hourlyOrderDF = ordersDF
    .groupBy("order_hour_of_day")
    .agg(count("*").alias("order_count"))
    .orderBy(desc("order_count"))
  hourlyOrderDF.show()

  // The day of the week with the highest number of orders
  val orderCountByDOWDF = ordersDF
    .groupBy("order_dow")
    .agg(count("*").alias("order_count"))
    .orderBy(desc("order_count"))
  orderCountByDOWDF.show()

  //Find the total number of products ordered by each user
  val joinedDF = ordersDF
    .join(order_products_priorDF, "order_id")

  val totalProductsByUserDF = joinedDF
    .groupBy("user_id")
    .agg(count("product_id").alias("total_products"))
  totalProductsByUserDF.show()

  //Find the top 10 users by the number of orders
  val orderCountByUserDF = ordersDF
    .groupBy("user_id")
    .agg(count("*").alias("order_count"))
    .orderBy(desc("order_count"))
    .limit(10)
  orderCountByUserDF.show()

  //Find all products ordered at least 5 times
  val productOrderCountDF = order_products_priorDF
    .groupBy("product_id")
    .agg(count("*").alias("order_count"))
    .filter(col("order_count") >= 5)

  productOrderCountDF.show()

  // Find users who have placed more than 10 orders
  val userOrderCountDF = ordersDF
    .groupBy("user_id")
    .agg(count("*").alias("order_count"))
    .filter(col("order_count") > 10)
  userOrderCountDF.show()


}
