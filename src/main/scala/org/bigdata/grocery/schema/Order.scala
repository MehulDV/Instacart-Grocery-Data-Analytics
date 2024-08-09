package org.bigdata.grocery.schema

case class Order(
                  order_id: Int,
                  user_id: Int,
                  eval_set: Option[String],
                  order_number: Option[Int],
                  order_dow: Option[Int],
                  order_hour_of_day: Option[Int],
                  days_since_prior_order: Option[Int]
                )
