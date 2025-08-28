def customer_columns():
    return ["Customer Id", "Customer Email", "Customer Fname", "Customer Lname",
                    "Customer Segment", "Customer City", "Customer Country",
                    "Customer State", "Customer Street", "Customer Zipcode"]

def product_columns():
    return ["Product Card Id", "Product Category Id", "Category Name", "Product Description",
                   "Product Image", "Product Name", "Product Price", "Product Status"]

def location_columns():
    return ["Order Zipcode", "Order City", "Order State", "Order Region","Order Country",
                    "Latitude", "Longitude"]

def order_columns():
    return ["Order Id","Order date (DateOrders)", "Order Customer Id", "Order Item Id","Product Card Id",
                "Order Item Discount", "Order Item Discount Rate", "Order Item Product Price",
                "Order Item Profit Ratio", "Order Item Quantity", "Sales per customer", "Sales",
                "Order Item Total", "Order Profit Per Order", "Order Status","Department Id"]

def shipping_columns():
    return ["Shipping date (DateOrders)", "Days for shipping (real)", "Days for shipment (scheduled)",
                    "Shipping Mode","Delivery Status"]

def department_columns():
    return ["Department Id", "Department Name" ,"Market"]

def metadata_columns():
    return ["key","offset","partition","time","topic"]
