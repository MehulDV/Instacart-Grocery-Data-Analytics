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

  productsDF.printSchema()
  ordersDF.printSchema()
  aislesDF.printSchema()
  departmentDF.printSchema()
  order_products_priorDF.printSchema()
  order_products_trainDF.printSchema()

  //  val productsDF = spark.read.schema(productsSchema).parquet(productsFilePath)
//  val ordersDF = spark.read.schema(ordersSchema).parquet(orders_FilePath)
//  val aislesDF = spark.read.schema(aislesSchema).parquet(aislesFilePath)
//  val departmentDF = spark.read.schema(departmentsSchema).parquet(departmentsFilePath)
//  val order_products_priorDF = spark.read.schema(orderProductsPriorSchema).parquet(order_products_priorFilePath)
//  val order_products_trainDF = spark.read.schema(orderProductsTrainSchema).parquet(order_products_trainFilePath)

  println("Products Count ", productsDF.count())
//  println("Orders Count ", ordersDF.count())

  // Filtering data using Where & Filter
  val productNamesDFUsingWhere = productsDF
    .where(col("aisle_id") === 1)
    .select(col("product_name"))

  val productNamesDFUsingFilter = productsDF
    .filter(col("aisle_id") === 1)
    .select("product_name")

//  val orderCountDF = ordersDF
//    .groupBy(col("user_id"))
//    .count()
//    .withColumnRenamed("count", "order_count")
//
//  orderCountDF.show()
//
//  val order_dow_CountDF = ordersDF
//    .groupBy(col("order_dow"))
//    .count()
//    .withColumnRenamed("count", "order_dow_count")
//  order_dow_CountDF.show()

//  SELECT product_id, add_to_cart_order FROM order_products_prior
  //  WHERE order_id = 10 ORDER BY add_to_cart_order;

  val cartOrderDF = order_products_priorDF
    .filter(col("order_id") === 10)
    .select(
      col("product_id"),
      col("add_to_cart_order").cast("int").as("add_to_cart_order"))
    .orderBy(asc("add_to_cart_order"))
  cartOrderDF.show()

  val productCartOrderDF = order_products_priorDF
    .filter(col("order_id") === 10)
    .select("product_id", "add_to_cart_order")
    .orderBy("add_to_cart_order")
  productCartOrderDF.show()

}
