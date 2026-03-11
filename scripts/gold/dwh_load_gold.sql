/*===============================================================================
dwh_load_gold.sql (MySQL) - Data Warehousing Project
===============================================================================

Purpose:
  Build/refresh Gold layer analytics objects (dimensions + fact) as VIEWS
  from the Silver layer.

Objects created/refreshed:
  - gold_dim_customers (VIEW)
  - gold_dim_products  (VIEW)
  - gold_fact_sales    (VIEW)

Monitoring:
  - Tracks per-object build duration + total job duration.

===============================================================================*/

USE dwh;

-- ============================================================
-- ETL duration tracking (per-view + total)
-- ============================================================
DROP TEMPORARY TABLE IF EXISTS etl_load_metrics;
CREATE TEMPORARY TABLE etl_load_metrics (
  object_name   VARCHAR(150)  NOT NULL,
  started_at    DATETIME(6)   NOT NULL,
  ended_at      DATETIME(6)   NOT NULL,
  duration_sec  DECIMAL(12,6) NOT NULL
);

SET @job_start := NOW(6);

-- =============================================================================
-- 1) Gold Dimension: Customers
-- =============================================================================
SET @t_start := NOW(6);

-- Rename all columns to friendly names.
-- Order all columns.
-- As this is the dimension table so it also have the primary key.
-- Here insert the surrogate key if there is no primary key in the table.
-- Create a view for this object.

CREATE OR REPLACE VIEW gold_dim_customers AS
SELECT
  ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
  ci.cst_id  AS customer_id,
  ci.cst_key AS customer_number,
  ci.cst_firstname AS first_name,
  ci.cst_lastname  AS last_name,
  la.cntry          AS country,
  ci.cst_marital_status AS marital_status,
  CASE
    WHEN cst_gndr != 'n/a' THEN cst_gndr
    ELSE COALESCE(ca.gen, 'n/a')
  END AS gender,
  ca.bdate          AS birth_date,
  ci.cst_create_date AS create_date
FROM silver_crm_cust_info AS ci
LEFT JOIN silver_erp_cust_az12 AS ca
  ON ci.cst_key = ca.cid
LEFT JOIN silver_erp_loc_a101 AS la
  ON ci.cst_key = la.cid;

SET @t_end := NOW(6);
INSERT INTO etl_load_metrics(object_name, started_at, ended_at, duration_sec)
VALUES ('gold_dim_customers (VIEW)', @t_start, @t_end,
        TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end) / 1000000);


-- =============================================================================
-- 2) Gold Dimension: Products
-- =============================================================================
SET @t_start := NOW(6);

-- Rename all columns to friendly names.
-- Order all columns.
-- As this is the dimension table so it also have the primary key.
-- Here insert the surrogate key if there is no primary key in the table.
-- Create a view for this object.

CREATE OR REPLACE VIEW gold_dim_products AS
SELECT
  ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key,
  pi.prd_id  AS product_id,
  pi.prd_key AS product_number,
  pi.prd_nm  AS product_name,
  pi.cat_id  AS category_id,
  pa.cat     AS category,
  pa.subcat  AS subcategory,
  pa.maintenance,
  pi.prd_cost AS cost,
  pi.prd_line AS product_line,
  pi.prd_start_dt AS start_date
FROM silver_crm_prd_info AS pi
LEFT JOIN silver_erp_px_cat_g1v2 AS pa
  ON pi.cat_id = pa.id
WHERE pi.prd_end_dt IS NULL;

SET @t_end := NOW(6);
INSERT INTO etl_load_metrics(object_name, started_at, ended_at, duration_sec)
VALUES ('gold_dim_products (VIEW)', @t_start, @t_end,
        TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end) / 1000000);


-- =============================================================================
-- 3) Gold Fact: Sales
-- =============================================================================
SET @t_start := NOW(6);

-- As this is the fact table so over here we need to connect dimensions with this table.
-- Here use the dimension's surrogate keys instead of IDs to easily connect facts with dimensions.
-- Rename all columns to friendly names. 
-- Order all columns.
-- Create a view for this object.

CREATE OR REPLACE VIEW gold_fact_sales AS
SELECT
  sd.sls_ord_num AS order_number,
  pr.product_key,
  cu.customer_key,
  sd.sls_order_dt AS order_date,
  sd.sls_ship_dt  AS shipping_date,
  sd.sls_due_dt   AS due_date,
  sd.sls_sales    AS sales_amount,
  sd.sls_quantity AS quantity,
  sd.sls_price    AS price
FROM silver_crm_sales_details AS sd
LEFT JOIN gold_dim_products AS pr
  ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold_dim_customers AS cu
  ON sd.sls_cust_id = cu.customer_id;

SET @t_end := NOW(6);
INSERT INTO etl_load_metrics(object_name, started_at, ended_at, duration_sec)
VALUES ('gold_fact_sales (VIEW)', @t_start, @t_end,
        TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end) / 1000000);


-- =============================================================================
-- Final reporting: per-object + total duration
-- =============================================================================
SET @job_end := NOW(6);

SELECT
  object_name,
  started_at,
  ended_at,
  ROUND(duration_sec, 3) AS duration_sec
FROM etl_load_metrics
ORDER BY started_at;

SELECT
  'TOTAL' AS object_name,
  @job_start AS started_at,
  @job_end   AS ended_at,
  ROUND(TIMESTAMPDIFF(MICROSECOND, @job_start, @job_end) / 1000000, 3) AS duration_sec;
