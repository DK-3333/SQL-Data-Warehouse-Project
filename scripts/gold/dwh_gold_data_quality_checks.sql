/*===============================================================================
dwh_gold_data_quality_checks.sql (MySQL) - Data Warehousing Project
===============================================================================

Purpose:
  Validate Gold layer integrity and sanity after building/refeshing Gold views.
  This script also includes key Silver-layer business-rule validations that
  impact Gold modeling (e.g., gender master-source logic, current-product filter).

Checks included:
  - Row count sanity checks (Gold)
  - Null surrogate keys in fact (Gold)
  - Missing dimension matches for fact rows (Gold)
  - Duplicate key checks (Gold)
  - Gender consistency checks (Silver inputs for Gold customers)
  - Current product record checks (Silver inputs for Gold products)
  - Quick samples (Gold)

===============================================================================*/

USE dwh;

-- =============================================================================
-- 0) Silver-layer validations that impact Gold logic
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Customer gender consistency (CRM vs ERP)
-- ---------------------------------------------------------------------------
-- Here there are two gender information sources:
--   - CRM: silver_crm_cust_info.cst_gndr (master source)
--   - ERP: silver_erp_cust_az12.gen (secondary source)

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

-- CRM is master source for customer gender (fallback to ERP when CRM is 'n/a')
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

-- ---------------------------------------------------------------------------
-- Current product record check (prd_end_dt IS NULL)
-- ---------------------------------------------------------------------------
-- Products with NULL end date represent current product records
SELECT
  pi.prd_id,
  pi.prd_start_dt AS "current_date"
FROM silver_crm_prd_info AS pi
LEFT JOIN silver_erp_px_cat_g1v2 AS pa
  ON pi.cat_id = pa.id
WHERE pi.prd_end_dt IS NULL;  -- filter out all historical data

-- =============================================================================
-- 1) Quick sanity checks: row counts
-- =============================================================================
SELECT 'gold_dim_customers' AS object_name, COUNT(*) AS row_count FROM gold_dim_customers
UNION ALL
SELECT 'gold_dim_products',  COUNT(*) FROM gold_dim_products
UNION ALL
SELECT 'gold_fact_sales',    COUNT(*) FROM gold_fact_sales;

-- =============================================================================
-- 2) Null key checks in fact (should be none depending on data)
-- =============================================================================
SELECT *
FROM gold_fact_sales
WHERE customer_key IS NULL
   OR product_key IS NULL;

-- =============================================================================
-- 3) Missing dimension matches (referential integrity)
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

-- =============================================================================
-- 4) Duplicate checks 
-- =============================================================================
-- Duplicate customer surrogate keys (should not happen)
SELECT customer_key, COUNT(*) AS cnt
FROM gold_dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- Duplicate product surrogate keys (should not happen)
SELECT product_key, COUNT(*) AS cnt
FROM gold_dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- Duplicate order numbers (depends on business meaning)
SELECT order_number, COUNT(*) AS cnt
FROM gold_fact_sales
GROUP BY order_number
HAVING COUNT(*) > 1;

-- =============================================================================
-- 5) Quick samples
-- =============================================================================
SELECT * FROM gold_dim_customers LIMIT 25;
SELECT * FROM gold_dim_products  LIMIT 25;
SELECT * FROM gold_fact_sales    LIMIT 25;
