-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Incremental Data Loading
-- MAGIC

-- COMMAND ----------

CREATE DATABASE sales;

-- COMMAND ----------

CREATE OR REPLACE TABLE sales.Orders (
    OrderID INT,
    OrderDate DATE,
    CustomerID INT,
    CustomerName VARCHAR(100),
    CustomerEmail VARCHAR(100),
    ProductID INT,
    ProductName VARCHAR(100),
    ProductCategory VARCHAR(50),
    RegionID INT,
    RegionName VARCHAR(50),
    Country VARCHAR(50),
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    TotalAmount DECIMAL(10,2)
);


-- COMMAND ----------

INSERT INTO sales.Orders VALUES 
(1, '2024-02-01', 101, 'Alice Johnson', 'alice@example.com', 201, 'Laptop', 'Electronics', 301, 'North America', 'USA', 2, 800.00, 1600.00),
(2, '2024-02-02', 102, 'Bob Smith', 'bob@example.com', 202, 'Smartphone', 'Electronics', 302, 'Europe', 'Germany', 1, 500.00, 500.00),
(3, '2024-02-03', 103, 'Charlie Brown', 'charlie@example.com', 203, 'Tablet', 'Electronics', 303, 'Asia', 'India', 3, 300.00, 900.00),
(4, '2024-02-04', 101, 'Alice Johnson', 'alice@example.com', 204, 'Headphones', 'Accessories', 301, 'North America', 'USA', 1, 150.00, 150.00),
(5, '2024-02-05', 104, 'David Lee', 'david@example.com', 205, 'Gaming Console', 'Electronics', 302, 'Europe', 'France', 1, 400.00, 400.00),
(6, '2024-02-06', 102, 'Bob Smith', 'bob@example.com', 206, 'Smartwatch', 'Electronics', 303, 'Asia', 'China', 2, 200.00, 400.00),
(7, '2024-02-07', 105, 'Eve Adams', 'eve@example.com', 201, 'Laptop', 'Electronics', 301, 'North America', 'Canada', 1, 800.00, 800.00),
(8, '2024-02-08', 106, 'Frank Miller', 'frank@example.com', 207, 'Monitor', 'Accessories', 302, 'Europe', 'Italy', 2, 250.00, 500.00),
(9, '2024-02-09', 107, 'Grace White', 'grace@example.com', 208, 'Keyboard', 'Accessories', 303, 'Asia', 'Japan', 3, 100.00, 300.00),
(10, '2024-02-10', 104, 'David Lee', 'david@example.com', 209, 'Mouse', 'Accessories', 301, 'North America', 'USA', 1, 50.00, 50.00);


-- COMMAND ----------

SELECT * FROM sales.orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Data Warehousing

-- COMMAND ----------

CREATE DATABASE salesDWH;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Staging Layer

-- COMMAND ----------

-- Initial Load
CREATE OR REPLACE TABLE salesDWH.stg_sales 
AS 
SELECT * FROM sales.orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Transformation

-- COMMAND ----------

SELECT * FROM salesDWH.stg_sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Transformation View

-- COMMAND ----------

CREATE VIEW salesDWH.trans_sales
AS
SELECT * FROM salesDWH.stg_sales WHERE Quantity IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create DimCustomers View

-- COMMAND ----------

CREATE OR REPLACE VIEW salesDWH.view_DimCustomers
AS 
SELECT T.*, row_number() over(ORDER BY T.CustomerID) as DimCustomersKey 
FROM (
  SELECT 
    DISTINCT(CustomerID) as CustomerID,
    CustomerName,
    CustomerEmail
  FROM salesDWH.trans_sales
) AS T;

CREATE OR REPLACE TABLE salesDWH.DimCustomers (
    CustomerID INT,
    CustomerName VARCHAR(100),
    CustomerEmail VARCHAR(100),
    DimCustomersKey BIGINT
);

-- COMMAND ----------

INSERT INTO salesdwh.DimCustomers 
SELECT * FROM salesdwh.view_DimCustomers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query Transformation Data

-- COMMAND ----------

SELECT * FROM salesdwh.trans_sales;