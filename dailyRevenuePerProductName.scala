/* SCALA-SPARK using Dataframe and SQL

Problem Statement: Get daily revenue per product considering completed and closed orders. Save the result in HDFS location with JSON format
Data files
Orders : orderId, order_date, order_customer_id and order_status details in the text file in HDFS
orderItems : order_item_id, order_item_order_id, order_item_product_id, order_item_quantity, order_item_subtotal, order_item_product_price details in the text file in HDFS
products: product_id, product_category_id, product_name, product_description, product_price, product_image details in the text file in Local file system

*/

//Creating the orders data frame
val ordersRDD = sc.textFile("/user/cloudera/retail_db/orders")

val orderDF = ordersRDD.map(order => {(order.split(",")(0).toInt, order.split(",")(1), order.split(",")(2).toInt, order.split(",")(3))}).toDF("order_Id","order_date","order_customer_id","order_status")

orderDF.registerTempTable("orders")

//creating order items data frame

val order_itemsRDD = sc.textFile("/user/cloudera/retail_db/order_items")
val orderItemsDF = order_itemsRDD.map(ot => {
(ot.split(",")(0).toInt,ot.split(",")(1).toInt,ot.split(",")(2).toInt,ot.split(",")(3).toInt,ot.split(",")(4).toFloat,ot.split(",")(5).toFloat)
}).toDF("order_item_id","order_item_order_id","order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price")

orderItemsDF.registerTempTable("order_items")

//Creating products data frame

val productRaw = scala.io.Source.fromFile("/home/cloudera/Downloads/data-master/retail_db/products/part-00000").getLines.toList
val productRsDD = sc.parallelize(productRaw)
val productsDF = productsRDD.map(product => {(product.split(",")(0), product.split(",")(2))}).toDF("product_id","product_name")

productsDF.registerTempTable("products")

//Writing SQL query to join the orders, order_items and products table and getting the daily revenue per product and date.

sqlContext.sql("SELECT o.order_date, p.product_name, sum(ot.order_item_subtotal)as daily_revenue_per_product " +
"FROM orders o JOIN order_items ot " +
"ON o.order_id = ot.order_item_product_id " +
"JOIN products p ON p.product_id = ot.order_item_product_id " +
"WHERE o.order_status in ('COMPLETED','CLOSED') " +
"GROUP BY o.order_date, p.product_name " +
"ORDER BY o.order_date, daily_revenue_per_product desc").save("/user/nischal/daily_revenue_per_product_scala", "json")

