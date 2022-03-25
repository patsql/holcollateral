-- 1) Navigate to Data hub > Linked > Azure Data Lake Storage Gen2 > default storage account > raw container, to locate the 2 Parquet files 
-- Query Fact.Order parquet file: right-click on the file > New SQL script > SELECT TOP 100 rows
-- Run the auto-generated code - it looks like the one below: 
-- Obs: until step 4 everyhting can be run on serverless master db

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://sasynapseinaday1612.dfs.core.windows.net/raw/Fact/Order/Fact.Order.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]

-- We can select certain columns, do group by, all while still using the OPENROWSET statement:
SELECT
    OrderDateKey, StockItemKey, Description,
    CAST(SUM(TotalIncludingTax) AS decimal(18,2)) AS [(sum) TotalIncludingTax],
    CAST(AVG(TotalIncludingTax) AS decimal(18,2)) AS [(avg) TotalIncludingTax],
    SUM(Quantity) AS [(sum) Quantity]
FROM
    OPENROWSET(
        BULK 'https://sasynapseinaday1612.dfs.core.windows.net/raw/Fact/Order/Fact.Order.parquet',
        FORMAT='PARQUET'
    ) AS [result]
    GROUP BY OrderDateKey, StockItemKey, Description

-- 2) From raw container query Dimension.City parquet file, just like how you did above

-- This is auto-generated code
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://sasynapseinaday1612.dfs.core.windows.net/raw/Dimension/City/Dimension.City.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]


-- 3) Run joins between the 2 parquet files
SELECT
    OrderDateKey, Description as Product, City,
    CAST(SUM(TotalIncludingTax) AS decimal(18,2)) AS [(sum) TotalIncludingTax],
    CAST(AVG(TotalIncludingTax) AS decimal(18,2)) AS [(avg) TotalIncludingTax],
    SUM(Quantity) AS [(sum) Quantity]
FROM
    OPENROWSET(
        BULK 'https://sasynapseinaday1612.dfs.core.windows.net/raw/Fact/Order/Fact.Order.parquet',
        FORMAT='PARQUET'
    ) AS [result],
    OPENROWSET(
        BULK 'https://sasynapseinaday1612.dfs.core.windows.net/raw/Dimension/City/Dimension.City.parquet',
        FORMAT='PARQUET'
    ) AS [result1] 
    WHERE [result].citykey = [result1].citykey
    GROUP BY OrderDateKey,  Description, City

-- 4) Create 2 external tables for: Dimension.City and Fact.Order
/*
Benefits of EXTERNAL TABLE in SQL serverless:
- Rather than creating a script with OPENROWSET and a path to the root folder every time we want to query the Parquet files,
we can create an external table.
- External tables = convenient way to persist the schema of data residing in your data lake which can be reused for future adhoc analytics
- running the same External Table twice --> Query Optimizer knows that you are referencing the same object twice, 
- running the same external OPENROWSETs twice --> Query Optimizer won't recognize as the same object. 
Better execution plans could be generated when using external tables instead of OPENROWSETs, and also views using openrowset
*/

/*
Steps to create the external table:
- right-click on the Parquet file > new sql script > create external table
- create a new SQL Serverless database, called DEMO, and name the external tables DimCity and FactOrder
- choose the option "using SQL script" which will generate a SQL script
- analyze the script and run it: this will create the file format, the external Data Source if needed, create the external table and select the TOP 100
- Explore the DEMO db, located in the Data hub > Workspace > SQL database

*/

-- 5) From the Step 3, we will re-run the same query, but using the newly external tables created:
-- make sure you are on the DEMO db
SELECT top 100
    OrderDateKey, Description as Product, City,
    CAST(SUM(TotalIncludingTax) AS decimal(18,2)) AS [(sum) TotalIncludingTax], -- total price of the orders
    CAST(AVG(TotalIncludingTax) AS decimal(18,2)) AS [(avg) TotalIncludingTax],
    SUM(Quantity) AS [(sum) Quantity]
  FROM DimCity D
  JOIN FactOrder F on F.citykey = D.citykey
 GROUP BY OrderDateKey, Description, City

-- 6) Run the following query to show the result as bar chart
/*
Chart type:  Bar
Category column:  StateProvince.
Legend (series) columns:  (avg) TotalIncludingTax.
*/
-- Total price of the orders in a state
 SELECT TOP 10
    StateProvince,
    CAST(SUM(TotalIncludingTax) AS decimal(18,2)) AS [(sum) TotalIncludingTax],
    CAST(AVG(TotalIncludingTax) AS decimal(18,2)) AS [(avg) TotalIncludingTax]
  FROM DimCity D
  JOIN FactOrder F on F.citykey = D.citykey
 GROUP BY Description, StateProvince
 ORDER BY 3 DESC

-- 7) Show cost control on the built-in serverless pool






