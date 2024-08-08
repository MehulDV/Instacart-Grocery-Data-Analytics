import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object ReadCSVFiles {
  System.setProperty("hadoop.home.dir", "C:\\Hadoop\\")

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("Grocery Data Analysis")
      .getOrCreate()

//    val aislesDF = spark.read.option("header","true").csv("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\csv\\aisles.csv")
//
//    val departmentsDF = spark.read.option("header","true").csv("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\csv\\departments.csv")
//
//    val order_products_prior_DF = spark.read.option("header","true").csv("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\csv\\order_products__prior.csv")
//
//    val order_products_train_DF = spark.read.option("header","true").csv("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\csv\\order_products__train.csv")
//
//    val ordersDF = spark.read.option("header","true").csv("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\csv\\orders.csv")
//
//    val productsDF = spark.read.option("header","true").csv("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\csv\\products.csv")
//
//
//    aislesDF.write.mode("overwrite").parquet("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\parquet\\aisles.parquet")
//    departmentsDF.write.mode("overwrite").parquet("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\parquet\\departments.parquet")
//    order_products_prior_DF.write.mode("overwrite").parquet("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\parquet\\order_products_prior.parquet")
//    order_products_train_DF.write.mode("overwrite").parquet("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\parquet\\order_products_train.parquet")
//    ordersDF.write.mode("overwrite").parquet("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\parquet\\orders.parquet")
//    productsDF.write.mode("overwrite").parquet("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\parquet\\products.parquet")


    val aislesDF = spark.read.parquet("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\parquet\\aisles.parquet")
    val departmentsDF = spark.read.parquet("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\parquet\\departments.parquet")
    val order_products_prior_DF = spark.read.parquet("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\parquet\\order_products_prior.parquet")
    val order_products_train_DF = spark.read.parquet("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\parquet\\order_products_train.parquet")
    val ordersDF = spark.read.parquet("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\parquet\\orders.parquet")
    val productsDF = spark.read.parquet("C:\\Users\\Mehul Vekariya\\Desktop\\GroceryDataAnalysis\\src\\resources\\parquet\\products.parquet")

    aislesDF.count()
    departmentsDF.count()
    order_products_prior_DF.count()
    order_products_train_DF.count()
    ordersDF.count()
    productsDF.count()

    println("aislesDF.count()", aislesDF.count())
    println("departmentsDF.count()", departmentsDF.count())
    println("order products prior.count()", order_products_prior_DF.count())
    println("order products train.count()", order_products_train_DF.count())
    println("ordersDF.count()", ordersDF.count())
    println("products.count()", productsDF.count())

  }
}
