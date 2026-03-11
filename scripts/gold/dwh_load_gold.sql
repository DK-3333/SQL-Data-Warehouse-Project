/*
===============================================================================
dwh_load_gold.sql (MySQL) - Data Warehousing Project
===============================================================================

Purpose:
  Create the Gold layer business objects for analytics and reporting.
  Gold layer exposes curated, analytics-ready objects (dimensions + facts)
  based on Silver layer cleaned/standardized tables.

Objects created:
  - gold_dim_customers (VIEW)
  - gold_dim_products  (VIEW)
  - gold_fact_sales    (VIEW)

Notes:
  - Gold objects are implemented as VIEWS (lightweight semantic layer).
  - Surrogate keys are generated using ROW_NUMBER() for dimensions.
  - This script also includes validation queries for referential integrity.

===============================================================================
*/

USE dwh;

-- =============================================================================
-- BUSINESS OBJECT: CUSTOMER
-- =============================================================================

-- Start with master table: silver_crm_cust_info + enrich with ERP customer + location
SELECT
  ci.cst_id,
  ci.cst_key,
  ci.cst_firstname,
  ci.cst_lastname,
  ci.cst_marital_status,
  ci.cst_gndr,
  ci.cst_create_date,
  ca.bdate,
  ca.gen,
  la.cntry
FROM silver_crm_cust_info AS ci
LEFT JOIN silver_erp_cust_az12 AS ca -- Join the data with another table that is 'silver_erp_cust_az12'
  ON ci.cst_key = ca.cid
LEFT JOIN silver_erp_loc_a101 AS la -- Join the data with another table that is 'silver_erp_loc_a101'
  ON ci.cst_key = la.cid;

-- Here there are two gender information one from CRM and another from ERP

-- Gender consistency check (CRM vs ERP)
SELECT DISTINCT
  ci.cst_gndr,
  ca.gen
FROM silver_crm_cust_info AS ci
LEFT JOIN silver_erp_cust_az12 AS ca
  ON ci.cst_key = ca.cid
LEFT JOIN silver_erp_loc_a101 AS la
  ON ci.cst_key = la.cid
ORDER BY 1, 2;

-- CRM is master source for customer gender
SELECT DISTINCT
  ci.cst_gndr,
  ca.gen,
  CASE
    WHEN cst_gndr != 'n/a' THEN cst_gndr
    ELSE COALESCE(ca.gen, 'n/a')
  END AS test
FROM silver_crm_cust_info AS ci
LEFT JOIN silver_erp_cust_az12 AS ca
  ON ci.cst_key = ca.cid
LEFT JOIN silver_erp_loc_a101 AS la
  ON ci.cst_key = la.cid
ORDER BY 1, 2;

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

-- Check view
SELECT * FROM gold_dim_customers;

-- =============================================================================
-- BUSINESS OBJECT: PRODUCT
-- =============================================================================

-- Start with master table: silver_crm_prd_info + enrich with ERP product category
SELECT
  pi.prd_id,
  pi.cat_id,
  pi.prd_key,
  pi.prd_nm,
  pi.prd_cost,
  pi.prd_line,
  pi.prd_start_dt,
  pi.prd_end_dt,
  pa.cat,
  pa.subcat,
  pa.maintenance
FROM silver_crm_prd_info AS pi
LEFT JOIN silver_erp_px_cat_g1v2 AS pa -- Join the data with another table that is 'silver_erp_px_cat_g1v2'
  ON pi.cat_id = pa.id;

-- Products with NULL end date represent current product records
SELECT
  pi.prd_id,
  pi.prd_start_dt AS "current_date"
FROM silver_crm_prd_info AS pi
LEFT JOIN silver_erp_px_cat_g1v2 AS pa
  ON pi.cat_id = pa.id
WHERE pi.prd_end_dt IS NULL; -- filter out all historical data

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

-- Check view
SELECT * FROM gold_dim_products;

-- =============================================================================
-- BUSINESS OBJECT: SALES
-- =============================================================================

-- Start with master table: silver_crm_sales_details
SELECT
  sd.sls_ord_num,
  sd.sls_prd_key,
  sd.sls_cust_id,
  sd.sls_order_dt,
  sd.sls_ship_dt,
  sd.sls_due_dt,
  sd.sls_sales,
  sd.sls_quantity,
  sd.sls_price
FROM silver_crm_sales_details AS sd;

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

-- Check view
SELECT * FROM gold_fact_sales;

-- =============================================================================
-- VALIDATIONS: Foreign Key Integrity (Dimensions)
-- =============================================================================

-- Facts with missing customer dimension match
SELECT *
FROM gold_fact_sales AS f
LEFT JOIN gold_dim_customers AS c
  ON c.customer_key = f.customer_key
WHERE c.customer_key IS NULL;

-- Facts with missing product dimension match
SELECT *
FROM gold_fact_sales AS f
LEFT JOIN gold_dim_products AS p
  ON p.product_key = f.product_key
WHERE p.product_key IS NULL;

-- Final quick checks
SELECT * FROM gold_dim_customers;
SELECT * FROM gold_dim_products;
SELECT * FROM gold_fact_sales;
