package org.bigdata.grocery.dataframe

import org.apache.spark.sql.functions._
import org.bigdata.grocery.dataframe.Aggregation._

object joins {

  // Finding users who have reordered the same product
  val reorderDF = ordersDF
    .join(order_products_priorDF, "order_id")
    .filter(col("reordered") === 1)

  val distinctUserIdsDF = reorderDF
    .select("user_id")
    .distinct()
  distinctUserIdsDF.show()

  //Finding users who have not reordered any products
  val orderIdJoin = ordersDF
    .join(order_products_priorDF, Seq("order_id"))

  val notReorderedDF = orderIdJoin
    .filter(col("reordered") === 0)
    .select("user_id")
    .distinct()
  notReorderedDF.show()

  //List all products with their aisle and department names
  val aisleDeptDF = productsDF
    .join(aislesDF, productsDF("aisle_id") === aislesDF("aisle_id"))
    .join(departmentDF, productsDF("department_id") === departmentDF("department_id"))
    .select(productsDF("product_name"), aislesDF("aisle"), departmentDF("department"))
  aisleDeptDF.show()

  // Find products that have never been reordered
  val productsOrderLeftJoinDF = productsDF
    .join(order_products_priorDF, productsDF("product_id") === order_products_priorDF("product_id"), "left")

  val filteredDF = productsOrderLeftJoinDF
    .filter(col("reordered").isNull || col("reordered") === 0)
    .select("product_name")

  filteredDF.show()

  //Find the most popular product in each department
  val joinedDF = departmentDF
    .join(productsDF, departmentDF("department_id") === productsDF("department_id"))
    .join(order_products_priorDF, productsDF("product_id") === order_products_priorDF("product_id"))

  val departmentProductCountDF = joinedDF
    .groupBy("department", "product_name")
    .agg(count("product_id").alias("product_count"))
    .orderBy(desc("product_count"))
  departmentProductCountDF.show()

  //Identify users who only order from a specific department:
  val ordersJoinDF = ordersDF
    .join(order_products_priorDF, ordersDF("order_id") === order_products_priorDF("order_id"))
    .join(productsDF, order_products_priorDF("product_id") === productsDF("product_id"))
    .join(departmentDF, productsDF("department_id") === departmentDF("department_id"))

  val deptCountDF = ordersJoinDF
    .groupBy("user_id", "department_id")
    .agg(countDistinct("department_id").alias("dept_count"))
    .filter(col("dept_count") === 1)
  deptCountDF.show()

  // Identify users who place the majority of their orders on weekends
  val weekendOrdersDF = ordersDF
    .filter(col("order_dow").isin(6, 7))
    .groupBy("user_id")
    .agg(count("*").alias("weekend_orders"))

  val totalOrdersDF = ordersDF
    .groupBy("user_id")
    .agg(count("*").alias("total_orders"))

  val weekendTotalOrderDF = weekendOrdersDF
    .join(totalOrdersDF, "user_id")

  val resultDF = weekendTotalOrderDF
    .filter(col("weekend_orders") > col("total_orders") / 2)
  resultDF.show()

  // Find the most frequently ordered product during lunch hours (12 PM - 1 PM)

  val productJoinedDF = productsDF
    .join(order_products_priorDF, productsDF("product_id") === order_products_priorDF("product_id"))
    .join(ordersDF, order_products_priorDF("order_id") === ordersDF("order_id"))

  val hourOrderedDF = productJoinedDF
    .filter(col("order_hour_of_day").between(12, 13))

  val productOrderCountDF = hourOrderedDF
    .groupBy("product_name")
    .agg(count("product_id").alias("order_count"))
    .orderBy(desc("order_count"))
  productOrderCountDF.show()


}
