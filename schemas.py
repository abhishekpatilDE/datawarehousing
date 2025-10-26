# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sales.Orders (
# MAGIC   OrderID INT,
# MAGIC   OrderDate DATE,
# MAGIC   CustomerID INT,
# MAGIC   CustomerName STRING,
# MAGIC   CustomerEmail STRING,
# MAGIC   ProductID INT,
# MAGIC   ProductName STRING,
# MAGIC   ProductCategory STRING,
# MAGIC   RegionID INT,
# MAGIC   RegionName STRING,
# MAGIC   Country STRING,
# MAGIC   Quantity INT,
# MAGIC   UnitPrice DECIMAL(10,2),
# MAGIC   TotalAmount DECIMAL(10,2)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sales.Orders (OrderID, OrderDate, CustomerID, CustomerName, CustomerEmail, ProductID, ProductName, ProductCategory, RegionID, RegionName, Country, Quantity, UnitPrice, TotalAmount)
# MAGIC VALUES
# MAGIC (1, '2022-01-01', 101, 'AliceJohnson', 'alice@example.com', 201, 'Laptop', 'Electronics', 301, 'NorthAmerica', 'USA', 2, 800.00, 1600.00),
# MAGIC (2, '2022-01-05', 102, 'BobSmith', 'bobsmith@example.com', 202, 'Smartphone', 'Electronics', 302, 'Europe', 'Germany', 1, 600.00, 600.00),
# MAGIC (3, '2022-01-07', 103, 'CarolWhite', 'carolw@example.com', 203, 'Washing Machine', 'HomeAppliance', 303, 'Asia', 'India', 1, 450.00, 450.00),
# MAGIC (4, '2022-01-10', 104, 'DavidBrown', 'davidb@example.com', 204, 'Running Shoes', 'Footwear', 304, 'Europe', 'France', 3, 120.00, 360.00),
# MAGIC (5, '2022-01-12', 105, 'EmmaDavis', 'emmad@example.com', 205, 'Coffee Maker', 'HomeAppliance', 305, 'NorthAmerica', 'Canada', 1, 150.00, 150.00),
# MAGIC (6, '2022-01-15', 106, 'FrankMiller', 'frankm@example.com', 206, 'Office Chair', 'Furniture', 306, 'Europe', 'Italy', 2, 200.00, 400.00),
# MAGIC (7, '2022-01-18', 107, 'GraceWilson', 'gracew@example.com', 207, 'Bluetooth Speaker', 'Electronics', 307, 'Asia', 'Japan', 1, 100.00, 100.00),
# MAGIC (8, '2022-01-20', 108, 'HenryMoore', 'henrym@example.com', 208, 'Backpack', 'Accessories', 308, 'Europe', 'Spain', 2, 80.00, 160.00),
# MAGIC (9, '2022-01-22', 109, 'IsabellaClark', 'isabellac@example.com', 209, 'Smartwatch', 'Electronics', 309, 'NorthAmerica', 'Mexico', 1, 250.00, 250.00),
# MAGIC (10, '2022-01-25', 110, 'JackLewis', 'jackl@example.com', 210, 'Desk Lamp', 'HomeDecor', 310, 'Asia', 'Singapore', 3, 45.00, 135.00),
# MAGIC (11, '2022-01-27', 111, 'KarenWalker', 'karenw@example.com', 211, 'Microwave', 'HomeAppliance', 311, 'Europe', 'Sweden', 1, 300.00, 300.00),
# MAGIC (12, '2022-01-29', 112, 'LiamHall', 'liamh@example.com', 212, 'Gaming Console', 'Electronics', 312, 'NorthAmerica', 'USA', 1, 500.00, 500.00),
# MAGIC (13, '2022-02-01', 113, 'MiaYoung', 'miay@example.com', 213, 'Winter Jacket', 'Clothing', 313, 'Europe', 'Norway', 2, 180.00, 360.00),
# MAGIC (14, '2022-02-03', 114, 'NoahKing', 'noahk@example.com', 214, 'Tablet', 'Electronics', 314, 'Asia', 'SouthKorea', 1, 400.00, 400.00),
# MAGIC (15, '2022-02-05', 115, 'OliviaScott', 'olivias@example.com', 215, 'Perfume', 'Beauty', 315, 'Europe', 'Switzerland', 2, 90.00, 180.00);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales.Orders

# COMMAND ----------

# MAGIC %md
# MAGIC #DATAWAREHOUSE

# COMMAND ----------

# MAGIC %sql
# MAGIC create database salesdwh

# COMMAND ----------

# MAGIC %md
# MAGIC ## STAGING LAYER

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE salesdwh.stg_sales
# MAGIC AS
# MAGIC SELECT * FROM sales.orders

# COMMAND ----------

# MAGIC %md
# MAGIC ##Transformation

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from salesdwh.stg_sales
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW salesdwh.vw_sales_tripled AS
# MAGIC SELECT
# MAGIC     OrderID,
# MAGIC     OrderDate,
# MAGIC     CustomerID,
# MAGIC     CustomerName,
# MAGIC     CustomerEmail,
# MAGIC     ProductID,
# MAGIC     ProductName,
# MAGIC     ProductCategory,
# MAGIC     RegionID,
# MAGIC     RegionName,
# MAGIC     Country,
# MAGIC     Quantity * 3 AS Quantity,
# MAGIC     UnitPrice,
# MAGIC     UnitPrice * (Quantity * 3) AS TotalAmount
# MAGIC FROM salesdwh.stg_sales;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## CORE Layer 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE salesdwh.core_sales
# MAGIC AS
# MAGIC select * from salesdwh.vw_sales_tripled

# COMMAND ----------

# MAGIC %md
# MAGIC ### Core Layer Display

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesdwh.core_sales