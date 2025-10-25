import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder     .appName("Incremental Load Example")     .config("spark.sql.catalogImplementation", "hive")     .enableHiveSupport()     .getOrCreate()
print("Spark Session created successfully.")

spark.sql("CREATE DATABASE IF NOT EXISTS sales_db")
databases = spark.sql("SHOW DATABASES")
databases.show()

spark.sql("USE sales_db")
spark.sql("""
CREATE TABLE IF NOT EXISTS Orders (
    OrderID STRING,
    OrderDate DATE,
    CustomerID STRING,
    CustomerName STRING,
    CustomerEmail STRING,
    ProductID STRING,
    ProductName STRING,
    ProductCategory STRING,
    RegionID STRING,
    RegionName STRING,
    Country STRING,
    Quantity INT,
    UnitPrice DOUBLE,
    TotalAmount DOUBLE
)
USING PARQUET
""")
spark.sql("""
INSERT INTO Orders VALUES
('ORD001', DATE '2025-10-25', 'CUST001', 'John Doe', 'john.doe@email.com', 'PRD001', 'iPhone 15', 'Electronics', 'R001', 'Europe West', 'Germany', 2, 1200.00, 2400.00),
('ORD002', DATE '2025-10-26', 'CUST002', 'Alice Smith', 'alice.smith@email.com', 'PRD002', 'AirPods Pro', 'Electronics', 'R002', 'North Europe', 'Sweden', 1, 250.00, 250.00),
('ORD003', DATE '2025-10-27', 'CUST003', 'Bob Lee', 'bob.lee@email.com', 'PRD003', 'Nike Shoes', 'Footwear', 'R003', 'South Europe', 'Italy', 3, 100.00, 300.00)
""")
spark.sql("SELECT * FROM Orders").show(truncate=False)

# Additional queries
spark.sql("SHOW TABLES").show()
spark.sql("SELECT * FROM Orders LIMIT 10").show()

# Staging area for incremental loads
spark.sql("CREATE DATABASE IF NOT EXISTS sales_db_staging")
spark.sql("USE sales_db")
spark.sql("SELECT * FROM Orders").show(truncate=False)

# Core Layer for final storage
spark.sql("CREATE DATABASE IF NOT EXISTS sales_db_core")
spark.sql("USE sales_db_core")
spark.sql("SELECT * FROM Orders").show(truncate=False)
