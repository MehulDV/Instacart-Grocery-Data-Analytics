package org.bigdata.grocery.dataframe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, col, udf}
import org.bigdata.grocery.utils.Constants

object DataFrameReader {
  
  System.setProperty("hadoop.home.dir", "C:\\Hadoop\\")

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("Grocery Data Analysis")
      .getOrCreate()

//    val aislesDF = spark.read.option("header","true").csv("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\csv\\aisles.csv")
//    val departmentsDF = spark.read.option("header","true").csv("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\csv\\departments.csv")
//    val order_products_prior_DF = spark.read.option("header","true").csv("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\csv\\order_products__prior.csv")
//    val order_products_train_DF = spark.read.option("header","true").csv("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\csv\\order_products__train.csv")
//    val ordersDF = spark.read.option("header","true").csv("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\csv\\orders.csv")
//    val productsDF = spark.read.option("header","true").csv("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\csv\\products.csv")
//
//    aislesDF.write.mode("overwrite").parquet("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\parquet\\aisles.parquet")
//    departmentsDF.write.mode("overwrite").parquet("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\parquet\\departments.parquet")
//    order_products_prior_DF.write.mode("overwrite").parquet("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\parquet\\order_products_prior.parquet")
//    order_products_train_DF.write.mode("overwrite").parquet("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\parquet\\order_products_train.parquet")
//    ordersDF.write.mode("overwrite").parquet("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\parquet\\orders.parquet")
//    productsDF.write.mode("overwrite").parquet("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\parquet\\products.parquet")

    val aislesDF = spark.read.parquet(Constants.aislesFilePath)
    val departmentsDF = spark.read.parquet(Constants.departmentsFilePath)
    val productsDF = spark.read.parquet(Constants.productsFilePath)
    val order_products_priorDF = spark.read.parquet(Constants.order_products_priorFilePath)
    val ordersDF = spark.read.parquet(Constants.orders_FilePath)
    val orderProductsTrainDF = spark.read.parquet(Constants.order_products_trainFilePath)

    val aisleDFUpdated = aislesDF
      .withColumn("aisle_id", col("aisle_id").cast("int"))
      .withColumn("aisle", col("aisle").cast("string"))

    val departmentDFUpdated = departmentsDF
      .withColumn("department_id", col("department_id").cast("int"))
      .withColumn("department", col("department").cast("string"))

    val departmentsDFUpdated = productsDF
      .withColumn("product_id", col("product_id").cast("int"))
      .withColumn("product_name", col("product_name").cast("string"))
      .withColumn("aisle_id", col("aisle_id").cast("int"))
      .withColumn("department_id", col("department_id").cast("int"))

    val order_products_prior_DF_Updated = order_products_priorDF
      .withColumn("order_id", col("order_id").cast("int"))
      .withColumn("product_id", col("product_id").cast("int"))
      .withColumn("add_to_cart_order", col("add_to_cart_order").cast("int"))
      .withColumn("reordered", col("reordered").cast("int"))

    val ordersDF_Updated = ordersDF
      .withColumn("order_id", col("order_id").cast("int"))
      .withColumn("user_id", col("user_id").cast("int"))
      .withColumn("eval_set", col("eval_set").cast("string"))
      .withColumn("order_number", col("order_number").cast("int"))
      .withColumn("order_dow", col("order_dow").cast("int"))
      .withColumn("order_hour_of_day", col("order_hour_of_day").cast("int"))
      .withColumn("days_since_prior_order", col("days_since_prior_order").cast("int"))

    val order_products_train_DF_updated = orderProductsTrainDF
      .withColumn("order_id", col("order_id").cast("int"))
      .withColumn("product_id", col("product_id").cast("int"))
      .withColumn("add_to_cart_order", col("add_to_cart_order").cast("int"))
      .withColumn("reordered", col("reordered").cast("int"))

    val productCartOrderDF = order_products_prior_DF_Updated
      .filter(col("order_id") === 10)
      .select("product_id", "add_to_cart_order")
      .orderBy(asc("add_to_cart_order"))
    productCartOrderDF.show()

  }
}
