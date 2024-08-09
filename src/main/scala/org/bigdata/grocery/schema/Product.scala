package org.bigdata.grocery.schema

case class Product(
                    product_id: Int,
                    product_name: Option[String],
                    aisle_id: Option[Int],
                    department_id: Option[Int]
                  )

