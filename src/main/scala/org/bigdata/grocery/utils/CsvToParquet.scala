package org.bigdata.grocery.utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, col, udf}
import org.bigdata.grocery.dataframe.Aggregation._
import org.bigdata.grocery.utils.Constants

object CsvToParquet {
  System.setProperty("hadoop.home.dir", "C:\\Hadoop\\")

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("Grocery Data Analysis")
      .getOrCreate()

//    val aislesDFWithSchema = spark.read
//      .option("header", true)
//      .schema(aislesSchema)
//      .csv("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\csv\\aisles.csv")

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

  }
}
