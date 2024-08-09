
import org.apache.spark.sql.types._

class InstaCartDbSchema {

  val aislesSchema: StructType = StructType(Array(
    StructField("aisle_id", IntegerType, nullable = false),
    StructField("aisle", StringType, nullable = true)
  ))

  val departmentsSchema: StructType = StructType(Array(
    StructField("department_id", IntegerType, nullable = false),
    StructField("department", StringType, nullable = true)
  ))

  val orderProductsPriorSchema: StructType = StructType(Array(
    StructField("order_id", IntegerType, nullable = false),
    StructField("product_id", IntegerType, nullable = false),
    StructField("add_to_cart_order", IntegerType, nullable = true),
    StructField("reordered", IntegerType, nullable = true)
  ))

  val orderProductsTrainSchema: StructType = StructType(Array(
    StructField("order_id", IntegerType, nullable = false),
    StructField("product_id", IntegerType, nullable = false),
    StructField("add_to_cart_order", IntegerType, nullable = true),
    StructField("reordered", IntegerType, nullable = true)
  ))

  val ordersSchema: StructType = StructType(Array(
    StructField("order_id", IntegerType, nullable = false),
    StructField("user_id", IntegerType, nullable = false),
    StructField("eval_set", StringType, nullable = true),
    StructField("order_number", IntegerType, nullable = true),
    StructField("order_dow", IntegerType, nullable = true),
    StructField("order_hour_of_day", IntegerType, nullable = true),
    StructField("days_since_prior_order", IntegerType, nullable = true)
  ))

  val productsSchema: StructType = StructType(Array(
    StructField("product_id", IntegerType, nullable = false),
    StructField("product_name", StringType, nullable = true),
    StructField("aisle_id", IntegerType, nullable = true),
    StructField("department_id", IntegerType, nullable = true)
  ))
}
