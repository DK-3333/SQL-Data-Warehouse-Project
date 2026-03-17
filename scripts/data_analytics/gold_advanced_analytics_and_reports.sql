/*
===============================================================================
gold_advanced_analytics_and_reports.sql (MySQL) - Data Warehousing Project
===============================================================================

Purpose:
  Advanced analytics on Gold layer objects plus creation of reporting views:
    - gold_report_customers
    - gold_report_products

Contents:
  1) Change-over-time analysis (year/month)
  2) Cumulative analysis (running totals, moving averages)
  3) Performance analysis (product vs avg, YoY comparison)
  4) Part-to-whole analysis (category contribution)
  5) Data segmentation (products by cost bands, customers by spend/tenure)
  6) Reporting views for downstream BI usage

Notes:
  - Requires MySQL 8+ (CTEs + window functions).
  - All analysis queries are read-only.
  - Views are created/updated using CREATE OR REPLACE VIEW.

===============================================================================
*/

USE dwh;

-- =============================================================================
-- 1) Change-over-time analysis
-- =============================================================================

-- Sales performance by year
SELECT
  YEAR(order_date) AS order_year,
  SUM(sales_amount) AS total_sales_per_year
FROM gold_fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY order_year;

-- Customer count by year
SELECT
  YEAR(order_date) AS order_year,
  COUNT(DISTINCT customer_key) AS customers_per_year
FROM gold_fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY order_year;

-- Sales performance by month (note: month alone mixes years)
SELECT
  MONTH(order_date) AS order_month,
  SUM(sales_amount) AS total_sales_per_month
FROM gold_fact_sales
WHERE order_date IS NOT NULL
GROUP BY MONTH(order_date)
ORDER BY order_month;

-- Customer count by month (note: month alone mixes years)
SELECT
  MONTH(order_date) AS order_month,
  COUNT(DISTINCT customer_key) AS customers_per_month
FROM gold_fact_sales
WHERE order_date IS NOT NULL
GROUP BY MONTH(order_date)
ORDER BY order_month;

-- =============================================================================
-- 2) Cumulative analysis
-- =============================================================================

-- Yearly totals + running total + moving average over time
SELECT
  r.*,
  SUM(r.total_sales_per_year) OVER (ORDER BY r.year_bucket) AS running_total_sales,
  AVG(r.avg_sales_per_year)  OVER (ORDER BY r.year_bucket) AS moving_average_sales
FROM (
  SELECT
    DATE_FORMAT(order_date, '%Y-01-01') AS year_bucket,
    SUM(sales_amount) AS total_sales_per_year,
    AVG(sales_amount) AS avg_sales_per_year
  FROM gold_fact_sales
  WHERE order_date IS NOT NULL
  GROUP BY DATE_FORMAT(order_date, '%Y-01-01')
) AS r
ORDER BY r.year_bucket;

-- Monthly totals + running total within each year + moving average within each year
SELECT
  r.*,
  SUM(r.total_sales_per_month) OVER (
    PARTITION BY YEAR(r.month_bucket)
    ORDER BY r.month_bucket
  ) AS running_total_sales,
  AVG(r.avg_sales_per_month) OVER (
    PARTITION BY YEAR(r.month_bucket)
    ORDER BY r.month_bucket
  ) AS moving_average_sales
FROM (
  SELECT
    DATE_FORMAT(order_date, '%Y-%m-01') AS month_bucket,
    SUM(sales_amount) AS total_sales_per_month,
    AVG(sales_amount) AS avg_sales_per_month
  FROM gold_fact_sales
  WHERE order_date IS NOT NULL
  GROUP BY DATE_FORMAT(order_date, '%Y-%m-01')
) AS r
ORDER BY r.month_bucket;

-- =============================================================================
-- 3) Performance analysis
-- =============================================================================

-- Analyze the yearly performance of products by comparing each product's sales
-- to both it's average sales performance and the previous year's sales.

WITH yearly_product_sales AS (
  SELECT
    s.product_key,
    p.product_name,
    DATE_FORMAT(s.order_date, '%Y-01-01') AS year_bucket,
    SUM(s.sales_amount) AS total_sales_per_product_per_year
  FROM gold_fact_sales AS s
  LEFT JOIN gold_dim_products AS p
    ON s.product_key = p.product_key
  WHERE s.order_date IS NOT NULL
  GROUP BY
    DATE_FORMAT(s.order_date, '%Y-01-01'),
    s.product_key,
    p.product_name
)
SELECT
  year_bucket,
  product_name,
  total_sales_per_product_per_year,
  AVG(total_sales_per_product_per_year) OVER (PARTITION BY product_key) AS avg_sales,
  (total_sales_per_product_per_year
    - AVG(total_sales_per_product_per_year) OVER (PARTITION BY product_key)
  ) AS avg_comparision,
  CASE
    WHEN (total_sales_per_product_per_year
          - AVG(total_sales_per_product_per_year) OVER (PARTITION BY product_key)
         ) > 0 THEN 'Above_average'
    WHEN (total_sales_per_product_per_year
          - AVG(total_sales_per_product_per_year) OVER (PARTITION BY product_key)
         ) < 0 THEN 'Below_average'
    ELSE 'No change'
  END AS average_change,
  LAG(total_sales_per_product_per_year) OVER (PARTITION BY product_key ORDER BY year_bucket) AS py_sales,
  (total_sales_per_product_per_year
    - LAG(total_sales_per_product_per_year) OVER (PARTITION BY product_key ORDER BY year_bucket)
  ) AS py_comparision,
  CASE
    WHEN (total_sales_per_product_per_year
          - LAG(total_sales_per_product_per_year) OVER (PARTITION BY product_key ORDER BY year_bucket)
         ) > 0 THEN 'Increase'
    WHEN (total_sales_per_product_per_year
          - LAG(total_sales_per_product_per_year) OVER (PARTITION BY product_key ORDER BY year_bucket)
         ) < 0 THEN 'Decrease'
    ELSE 'No change'
  END AS py_change
FROM yearly_product_sales
ORDER BY product_name, year_bucket;

-- =============================================================================
-- 4) Part-to-whole analysis
-- =============================================================================

-- Category contribution to overall sales
WITH sales_per_categories AS (
  SELECT
    p.category,
    SUM(s.sales_amount) AS total_sales_per_category
  FROM gold_fact_sales AS s
  LEFT JOIN gold_dim_products AS p
    ON s.product_key = p.product_key
  GROUP BY p.category
)
SELECT
  *,
  CONCAT((total_sales_per_category / SUM(total_sales_per_category) OVER ()) * 100, '%') AS category_percentage
FROM sales_per_categories
ORDER BY category_percentage DESC;

-- =============================================================================
-- 5) Data segmentation
-- =============================================================================

-- Segment products into cost ranges and 
-- count how many products fall into each segment.

SELECT
  r.cost_range,
  COUNT(r.product_key) AS number_of_products
FROM (
  SELECT
    product_key,
    product_name,
    cost,
    CASE
      WHEN cost < 100 THEN 'Below 100'
      WHEN cost BETWEEN 100 AND 500 THEN '100-500'
      WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
      ELSE 'Above 1000'
    END AS cost_range
  FROM gold_dim_products
) AS r
GROUP BY r.cost_range
ORDER BY number_of_products DESC;

-- Group customer into three segments based on their spending behavior:
-- VIP: Customers with at least 12 months of history and spending more then 5000.
-- Regular: Customers with at least 12 months of history but spending 5000 or less.
-- New: Customers with a lifespan less then 12 months.
-- And find the total number of customers by each group

SELECT
  r.customer_category,
  COUNT(r.customer_category) AS number_of_customers
FROM (
  SELECT
    customer_key,
    SUM(sales_amount) AS total_sales_per_customer,
    TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date)) AS month_difference,
    CASE
      WHEN TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date)) >= 12
           AND SUM(sales_amount) > 5000 THEN 'VIP'
      WHEN TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date)) >= 12
           AND SUM(sales_amount) <= 5000 THEN 'Regular'
      WHEN TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date)) < 12 THEN 'New'
    END AS customer_category
  FROM gold_fact_sales
  WHERE order_date IS NOT NULL
  GROUP BY customer_key
) AS r
GROUP BY r.customer_category
ORDER BY number_of_customers DESC;

-- Create a view for reporting

-- =============================
-- Build Customer Report
-- =============================

CREATE OR REPLACE VIEW gold_report_customers AS

WITH base_query AS (
  -- Base Query: retrieve core columns from Gold layer tables
  SELECT
    f.order_number,
    f.product_key,
    f.order_date,
    f.sales_amount,
    f.quantity,
    c.customer_key,
    c.customer_number,
    CONCAT(c.first_name, ' ', c.last_name) AS full_name,
    TIMESTAMPDIFF(YEAR, birth_date, CURRENT_DATE()) AS age
  FROM gold_fact_sales AS f
  LEFT JOIN gold_dim_customers AS c
    ON c.customer_key = f.customer_key
  WHERE f.order_date IS NOT NULL
),

customer_aggregation AS (
  -- Customer Aggregation: summarize key metrics at the customer level
  SELECT
    customer_key,
    customer_number,
    full_name,
    age,
    COUNT(DISTINCT order_number) AS total_orders,
    SUM(sales_amount) AS total_sales,
    SUM(quantity) AS total_quantity,
    COUNT(DISTINCT product_key) AS total_products,
    MAX(order_date) AS last_order_date,
    TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
  FROM base_query
  GROUP BY customer_key, customer_number, full_name, age
)

-- Final Query; combine customer results into one output
SELECT
  customer_key,
  customer_number,
  full_name,
  age,
  CASE
    WHEN age < 20 THEN 'Under 20'
    WHEN age BETWEEN 20 AND 29 THEN '20-29'
    WHEN age BETWEEN 30 AND 39 THEN '30-39'
    WHEN age BETWEEN 40 AND 49 THEN '40-49'
    ELSE '50 and above'
  END AS age_group,
  CASE
    WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
    WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
    WHEN lifespan < 12 THEN 'New'
  END AS customer_segment,
  last_order_date,

  -- KPI: Recency (months since last order)
  TIMESTAMPDIFF(MONTH, last_order_date, CURRENT_DATE()) AS recency,

  total_orders,
  total_sales,
  total_quantity,
  total_products,
  lifespan,

  -- KPI: Average order value (AOV)
  CASE
    WHEN total_orders = 0 THEN 0
    ELSE total_sales / total_orders
  END AS avg_order_value,

  -- KPI: Average monthly spend
  CASE
    WHEN lifespan = 0 THEN total_sales
    ELSE total_sales / lifespan
  END AS avg_monthly_spend
FROM customer_aggregation;

-- Preview
SELECT * FROM gold_report_customers;

-- =============================
-- Build Product Report View
-- =============================

CREATE OR REPLACE VIEW gold_report_products AS

WITH base_query AS (
  -- Base Query: retrieve core columns from Gold layer tables
  SELECT
    f.order_number,
    f.order_date,
    f.customer_key,
    f.sales_amount,
    f.quantity,
    p.product_key,
    p.product_name,
    p.category,
    p.subcategory,
    p.cost
  FROM gold_fact_sales AS f
  LEFT JOIN gold_dim_products AS p
    ON p.product_key = f.product_key
  WHERE f.order_date IS NOT NULL
),

product_aggregation AS (
  -- Product Aggregation: summarize key metrics at the product level
  SELECT
    product_key,
    product_name,
    category,
    subcategory,
    cost,
    TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
    MAX(order_date) AS last_sale_date,
    COUNT(DISTINCT order_number) AS total_orders,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(sales_amount) AS total_sales,
    SUM(quantity) AS total_quantity,
    ROUND(AVG(CAST(sales_amount AS DECIMAL(18,2)) / NULLIF(quantity, 0)), 1) AS avg_selling_price
  FROM base_query
  GROUP BY product_key, product_name, category, subcategory, cost
)
-- Final query: combine product results into one output
SELECT
  product_key,
  product_name,
  category,
  subcategory,
  cost,
  last_sale_date,

  -- KPI: Recency
  TIMESTAMPDIFF(MONTH, last_sale_date, CURRENT_DATE()) AS recency_in_months,

  CASE
    WHEN total_sales > 50000 THEN 'High Performer'
    WHEN total_sales > -10000 THEN 'Mid Range'
    ELSE 'Low Performer'
  END AS product_segment,

  lifespan,
  total_orders,
  total_sales,
  total_quantity,
  total_customers,
  avg_selling_price,

  -- KPI: Average order revenue (AOR)
  CASE
    WHEN total_orders = 0 THEN 0
    ELSE total_sales / total_orders
  END AS avg_order_revenue,

  -- KPI: Average monthly revenue
  CASE
    WHEN lifespan = 0 THEN total_sales
    ELSE total_sales / lifespan
  END AS avg_monthly_revenue
FROM product_aggregation;

-- Preview
SELECT * FROM gold_report_products;
