package org.bigdata.grocery.schema


case class OrderProductPrior(
                              order_id: Int,
                              product_id: Int,
                              add_to_cart_order: Option[Int],
                              reordered: Option[Int]
                            )

