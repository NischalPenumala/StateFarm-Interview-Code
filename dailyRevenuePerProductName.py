"""
PYTHON-SPARK : Without using SQL

Problem Statement: Get daily revenue per product considering completed and closed orders. Save the result in HDFS location with text format
Data files
Orders : orderId, order_date, order_customer_id and order_status details in the text file in HDFS
orderItems : order_item_id, order_item_order_id, order_item_product_id, order_item_quantity, order_item_subtotal, order_item_product_price details in the text file in HDFS
products: product_id, product_category_id, product_name, product_description, product_price, product_image details in the text file in Local file system
"""

# Creating the order RDD from the text file
orders = sc.textFile("/user/cloudera/retail_db/orders")
# Getting the orderId and order Date
ordersMap = ordersFilter.map(lambda o: (int(o.split(",")[0]), o.split(",")[1]))

# Creating the order Items RDD using text file and Filtering out the completed and closed orders
orderItems = sc.textFile("/user/cloudera/retail_db/order_items")
ordersFilter = orders.filter(lambda o: o.split(",")[3] in ["COMPLETE","CLOSED"])

# Geting order_item_order_id, order_item_product_id and order_item_subtotal
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]), (int(oi.split(",")[2]), float(oi.split(",")[4]))))

# Joing the orders(orderId) and order_items(order_item_order_id) 
ordersJoin = ordersMap.join(orderItemsMap)

# Getting the order date, product id and order item subtotal
ordersJoinMap = ordersJoin.map(lambda oj: ((oj[1][0], oj[1][1][0]), oj[1][1][1]))

# Calculating the aggregate on order_item_subtotal value, by using order date and product id as key, 
from operator import add
dailyRevenuePerProductId = ordersJoinMap.reduceByKey(add)

# Creating the product RDD from file which is stored in local file system
productRaw = open("/home/cloudera/Downloads/data-master/retail_db/products/part-00000").read().splitlines()
products = sc.parallelize(productRaw)
# Getting the productId and product name from the RDD
productMap = products.map(lambda p: (int(p.split(",")[0]), p.split(",")[2]))

# Making productid as Key and order date, subtotal as value inorder to join with productMap
dailyRevenuePerProductIdMap = dailyRevenuePerProductId.map(lambda dr: (dr[0][1],(dr[0][0], dr[1])))

# Joining the dailyRevenuePerProductIdMap(productId) and productMap(productId)
dailyRevenuePerProductJoin = dailyRevenuePerProductIdMap.join(productMap)

# Sorting the data: order date as ascending and subtotal as descending order
dailyRevenuePerProduct = dailyRevenuePerProductJoin.map(lambda t: ((t[1][0][0], -t[1][0][1]), t[1][0][0] + "," + t[1][1] + "," + str(t[1][0][1])))
dailyRevenuePerProductSorted = dailyRevenuePerProduct.sortByKey()

# Finally geting the final restuls: order date, subtotal and product name
dailyRevenuePerProductName = dailyRevenuePerProductSorted.map(lambda r: r[1])

# Saving the result text file in location
dailyRevenuePerProductName.saveAsTextFile("/user/nischal/daily_revenue_text_python")


