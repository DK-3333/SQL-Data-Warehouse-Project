/*
===============================================================================
gold_eda.sql (MySQL) - Data Warehousing Project
===============================================================================

Purpose:
  Exploratory Data Analysis (EDA) and business insights on Gold layer objects:
    - gold_dim_customers
    - gold_dim_products
    - gold_fact_sales

How to use:
  - Run section-by-section in MySQL Workbench.
  - All queries are read-only (no INSERT/UPDATE/DELETE).

Notes:
  - This script assumes database `dwh` exists and Gold views are already built.
  - Uses window functions (MySQL 8+).

===============================================================================
*/

USE dwh;

-- =============================================================================
-- 0) Explore Objects
-- =============================================================================
SHOW FULL TABLES;

DESCRIBE gold_dim_customers;
DESCRIBE gold_dim_products;
DESCRIBE gold_fact_sales;

-- =============================================================================
-- 1) Basic EDA
-- =============================================================================

-- Explore all countries
SELECT DISTINCT country
FROM gold_dim_customers
ORDER BY country;

-- Explore all categories
SELECT DISTINCT category
FROM gold_dim_products
ORDER BY category;

-- Explore category + subcategory + product names
SELECT DISTINCT category, subcategory, product_name
FROM gold_dim_products
ORDER BY category, subcategory, product_name;

-- =============================================================================
-- 2) Date Exploration (Earliest & Latest Dates)
-- =============================================================================

-- Boundaries of order_date + coverage
SELECT
  MIN(order_date) AS first_order_date,
  MAX(order_date) AS last_order_date,
  TIMESTAMPDIFF(YEAR,  MIN(order_date), MAX(order_date))  AS years_of_sales,
  TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date))  AS months_of_sales
FROM gold_fact_sales
WHERE order_date IS NOT NULL;

-- Youngest customer (max birth_date)
SELECT
  first_name,
  last_name,
  birth_date,
  TIMESTAMPDIFF(YEAR, birth_date, NOW()) AS age
FROM gold_dim_customers
WHERE birth_date = (SELECT MAX(birth_date) FROM gold_dim_customers);

-- Oldest customer (min birth_date)
SELECT
  first_name,
  last_name,
  birth_date,
  TIMESTAMPDIFF(YEAR, birth_date, NOW()) AS age
FROM gold_dim_customers
WHERE birth_date = (SELECT MIN(birth_date) FROM gold_dim_customers);

-- =============================================================================
-- 3) Measures Exploration (Core KPIs)
-- =============================================================================

-- Total sales
SELECT SUM(sales_amount) AS total_sales
FROM gold_fact_sales;

-- Total items sold
SELECT SUM(quantity) AS total_items
FROM gold_fact_sales;

-- Avg selling price
SELECT AVG(price) AS average_price
FROM gold_fact_sales;

-- Number of orders
SELECT COUNT(order_number) AS no_orders
FROM gold_fact_sales;

-- Number of products
SELECT COUNT(product_key) AS no_products
FROM gold_dim_products;

-- Number of customers
SELECT COUNT(customer_key) AS no_customers
FROM gold_dim_customers;

-- =============================================================================
-- 4) Final KPI Report (Single Output Table)
-- =============================================================================

SELECT 'Total_sales' AS measure_name, SUM(sales_amount) AS measure_value
FROM gold_fact_sales
UNION ALL
SELECT 'Number_of_customers_in_sales', COUNT(DISTINCT customer_key)
FROM gold_fact_sales
UNION ALL
SELECT 'Total_items', SUM(quantity)
FROM gold_fact_sales
UNION ALL
SELECT 'Average_price', AVG(price)
FROM gold_fact_sales
UNION ALL
SELECT 'Number_of_orders', COUNT(order_number)
FROM gold_fact_sales
UNION ALL
SELECT 'Number_of_products', COUNT(product_key)
FROM gold_dim_products
UNION ALL
SELECT 'Number_of_customers', COUNT(customer_key)
FROM gold_dim_customers;

-- =============================================================================
-- 5) Magnitude Analysis
-- =============================================================================

-- Number of customers by country
SELECT
  country,
  COUNT(customer_key) AS number_of_customers
FROM gold_dim_customers
GROUP BY country
ORDER BY number_of_customers DESC;

-- Number of customers by gender
SELECT
  gender,
  COUNT(customer_key) AS number_of_customers
FROM gold_dim_customers
GROUP BY gender
ORDER BY number_of_customers DESC;

-- Number of products by category
SELECT
  category,
  COUNT(product_key) AS number_of_products
FROM gold_dim_products
GROUP BY category
ORDER BY number_of_products DESC;

-- Average cost by category
SELECT
  category,
  AVG(cost) AS average_cost_by_category
FROM gold_dim_products
GROUP BY category
ORDER BY average_cost_by_category DESC;

-- Revenue by category
SELECT
  p.category,
  SUM(s.sales_amount) AS revenue_by_category
FROM gold_dim_products AS p
LEFT JOIN gold_fact_sales AS s
  ON p.product_key = s.product_key
GROUP BY p.category
ORDER BY revenue_by_category DESC;

-- Revenue by customer
SELECT
  s.customer_key,
  CONCAT(c.first_name, ' ', c.last_name) AS full_name,
  SUM(s.sales_amount) AS revenue_by_customer
FROM gold_fact_sales AS s
LEFT JOIN gold_dim_customers AS c
  ON c.customer_key = s.customer_key
GROUP BY s.customer_key, full_name
ORDER BY revenue_by_customer DESC;

-- Distribution of sold items across countries (%)
SELECT
  r.*,
  (r.items_per_country / SUM(r.items_per_country) OVER () * 100) AS percentage_of_distribution
FROM (
  SELECT
    c.country,
    SUM(s.quantity) AS items_per_country
  FROM gold_fact_sales AS s
  LEFT JOIN gold_dim_customers AS c
    ON c.customer_key = s.customer_key
  GROUP BY c.country
) AS r
ORDER BY items_per_country DESC;

-- =============================================================================
-- 6) Ranking Analysis
-- =============================================================================

-- Top 5 best-performing products by revenue
SELECT
  p.product_name,
  SUM(s.sales_amount) AS revenue_by_product
FROM gold_fact_sales AS s
LEFT JOIN gold_dim_products AS p
  ON p.product_key = s.product_key
GROUP BY p.product_name
ORDER BY revenue_by_product DESC
LIMIT 5;

-- Bottom 5 worst-performing products by revenue
SELECT
  p.product_name,
  SUM(s.sales_amount) AS revenue_by_product
FROM gold_fact_sales AS s
LEFT JOIN gold_dim_products AS p
  ON p.product_key = s.product_key
GROUP BY p.product_name
ORDER BY revenue_by_product ASC
LIMIT 5;

-- Top 10 customers by revenue
SELECT *
FROM (
  SELECT
    s.customer_key,
    CONCAT(c.first_name, ' ', c.last_name) AS full_name,
    SUM(s.sales_amount) AS revenue_by_customer,
    ROW_NUMBER() OVER (ORDER BY SUM(s.sales_amount) DESC) AS rank_customers
  FROM gold_fact_sales AS s
  LEFT JOIN gold_dim_customers AS c
    ON c.customer_key = s.customer_key
  GROUP BY s.customer_key, full_name
) AS r
WHERE r.rank_customers <= 10;

-- Bottom 3 customers by number of orders
SELECT *
FROM (
  SELECT
    s.customer_key,
    CONCAT(c.first_name, ' ', c.last_name) AS full_name,
    ROW_NUMBER() OVER (ORDER BY COUNT(s.order_number) ASC) AS rank_customers
  FROM gold_fact_sales AS s
  LEFT JOIN gold_dim_customers AS c
    ON c.customer_key = s.customer_key
  GROUP BY s.customer_key, full_name
) AS r
WHERE r.rank_customers <= 3;

