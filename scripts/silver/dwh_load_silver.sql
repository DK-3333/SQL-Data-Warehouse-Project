/*
===============================================================================
dwh_load_silver.sql (MySQL) - Data Warehousing Project
===============================================================================

Purpose:
  Transform and standardize data from the Bronze layer into the Silver layer.
  Silver applies cleaning rules, standardization, deduplication, and basic
  business logic corrections while keeping the data close to source semantics.

What this script does:
  - Truncates Silver tables
  - Inserts transformed data from Bronze tables into Silver tables
  - Tracks per-table load duration + total job duration

WARNING:
  TRUNCATE TABLE removes all rows from Silver tables before reloading.
===============================================================================
*/

USE dwh;

-- ============================================================
-- ETL duration tracking (per-table + total)
-- ============================================================

DROP TEMPORARY TABLE IF EXISTS etl_load_metrics;
CREATE TEMPORARY TABLE etl_load_metrics (
  table_name      VARCHAR(150)  NOT NULL,
  started_at      DATETIME(6)   NOT NULL,
  ended_at        DATETIME(6)   NOT NULL,
  duration_sec    DECIMAL(12,6) NOT NULL
);

SET @job_start := NOW(6);

-- ====================================
-- SOURCE: CRM TABLE: bronze_crm_cust_info
-- TARGET: silver_crm_cust_info
-- ====================================
SET @t_start := NOW(6);

TRUNCATE TABLE silver_crm_cust_info;

INSERT INTO silver_crm_cust_info (
  cst_id,
  cst_key,
  cst_firstname,
  cst_lastname,
  cst_marital_status,
  cst_gndr,
  cst_create_date
)
SELECT
  cst_id,
  cst_key,
  TRIM(cst_firstname) AS cst_firstname,
  TRIM(cst_lastname)  AS cst_lastname,
  CASE UPPER(TRIM(cst_marital_status))
    WHEN 'M' THEN "Married"
    WHEN 'S' THEN "Single"
    ELSE 'n/a'
  END cst_marital_status,
  CASE UPPER(TRIM(cst_gndr))
    WHEN 'M' THEN "Male"
    WHEN 'F' THEN "Female"
    ELSE 'n/a'
  END cst_gndr,
  cst_create_date
FROM (
  SELECT *,
         RANK() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS 'flag_test'
  FROM bronze_crm_cust_info
) AS t
WHERE t.flag_test = 1
  AND cst_id != 0;

SET @t_end := NOW(6);
INSERT INTO etl_load_metrics(table_name, started_at, ended_at, duration_sec)
VALUES ('silver_crm_cust_info', @t_start, @t_end,
        TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end) / 1000000);


-- =======================================
-- SOURCE: CRM TABLE: bronze_crm_prd_info
-- TARGET: silver_crm_prd_info
-- =======================================
SET @t_start := NOW(6);

TRUNCATE TABLE silver_crm_prd_info;

INSERT INTO silver_crm_prd_info (
  prd_id,
  cat_id,
  prd_key,
  prd_nm,
  prd_cost,
  prd_line,
  prd_start_dt,
  prd_end_dt
)
SELECT
  prd_id,
  REPLACE(SUBSTR(prd_key, 1, 5), "-", "_") AS 'cat_id',
  SUBSTR(prd_key, 7, LENGTH(prd_key))     AS 'prd_key',
  prd_nm,
  prd_cost,
  CASE UPPER(TRIM(prd_line))
    WHEN "R" THEN "Road"
    WHEN "S" THEN "Other Sales"
    WHEN "M" THEN "Mountain"
    WHEN "T" THEN "Touring"
    ELSE 'n/a'
  END AS prd_line,
  prd_start_dt,
  DATE_SUB(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt), INTERVAL 1 DAY) AS prd_end_dt
FROM bronze_crm_prd_info;

SET @t_end := NOW(6);
INSERT INTO etl_load_metrics(table_name, started_at, ended_at, duration_sec)
VALUES ('silver_crm_prd_info', @t_start, @t_end,
        TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end) / 1000000);

-- =============================================
-- SOURCE: CRM TABLE: bronze_crm_sales_details
-- TARGET: silver_crm_sales_details
-- =============================================
SET @t_start := NOW(6);

TRUNCATE TABLE silver_crm_sales_details;

INSERT INTO silver_crm_sales_details (
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  sls_order_dt,
  sls_ship_dt,
  sls_due_dt,
  sls_sales,
  sls_quantity,
  sls_price
)
WITH base AS (
  SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price,

    -- Step 1: normalize price first
    CASE
      WHEN sls_price IS NULL OR sls_price = 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
      WHEN sls_price < 0
        THEN ABS(sls_price)
      ELSE sls_price
    END AS fixed_price
  FROM bronze_crm_sales_details
)
SELECT
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  CASE
    WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
    ELSE CAST(sls_order_dt AS DATE)
  END AS sls_order_dt,
  CASE
    WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
    ELSE CAST(sls_ship_dt AS DATE)
  END AS sls_ship_dt,
  CASE
    WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
    ELSE CAST(sls_due_dt AS DATE)
  END AS sls_due_dt,
  CASE
    WHEN sls_sales IS NULL OR sls_sales <= 0
      OR sls_sales != sls_quantity * ABS(fixed_price)
    THEN sls_quantity * ABS(fixed_price)
    ELSE sls_sales
  END AS sls_sales,
  NULLIF(sls_quantity, 0) AS sls_quantity,
  fixed_price AS sls_price
FROM base;

SET @t_end := NOW(6);
INSERT INTO etl_load_metrics(table_name, started_at, ended_at, duration_sec)
VALUES ('silver_crm_sales_details', @t_start, @t_end,
        TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end) / 1000000);

-- =========================================
-- SOURCE: ERP TABLE: bronze_erp_cust_az12
-- TARGET: silver_erp_cust_az12
-- =========================================
SET @t_start := NOW(6);

TRUNCATE TABLE silver_erp_cust_az12;

INSERT INTO silver_erp_cust_az12 (
  cid,
  bdate,
  gen
)
SELECT
  CASE
    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
    ELSE cid
  END AS cid,

  CASE
    WHEN bdate > CURRENT_DATE() THEN NULL
    ELSE bdate
  END AS bdate,

  CASE
    WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
    WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
    ELSE 'n/a'
  END AS gen
FROM bronze_erp_cust_az12;

SET @t_end := NOW(6);
INSERT INTO etl_load_metrics(table_name, started_at, ended_at, duration_sec)
VALUES ('silver_erp_cust_az12', @t_start, @t_end,
        TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end) / 1000000);

-- ========================================
-- SOURCE: ERP TABLE: bronze_erp_loc_a101
-- TARGET: silver_erp_loc_a101
-- ========================================
SET @t_start := NOW(6);

TRUNCATE TABLE silver_erp_loc_a101;

INSERT INTO silver_erp_loc_a101 (
  cid,
  cntry
)
SELECT
  REPLACE(cid, '-', '') AS cid,
  CASE
    WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
    WHEN TRIM(cntry) IN ('DE') THEN 'Germany'
    WHEN cntry = '' OR cntry IS NULL THEN 'n/a'
    ELSE TRIM(cntry)
  END AS cntry
FROM bronze_erp_loc_a101;

SET @t_end := NOW(6);
INSERT INTO etl_load_metrics(table_name, started_at, ended_at, duration_sec)
VALUES ('silver_erp_loc_a101', @t_start, @t_end,
        TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end) / 1000000);

-- ======================================
-- SOURCE: ERP TABLE: bronze_erp_px_cat_g1v2
-- TARGET: silver_erp_px_cat_g1v2
-- ======================================
SET @t_start := NOW(6);

TRUNCATE TABLE silver_erp_px_cat_g1v2;

INSERT INTO silver_erp_px_cat_g1v2 (
  id,
  cat,
  subcat,
  maintenance
)
SELECT
  id,
  cat,
  subcat,
  maintenance
FROM bronze_erp_px_cat_g1v2;

SET @t_end := NOW(6);
INSERT INTO etl_load_metrics(table_name, started_at, ended_at, duration_sec)
VALUES ('silver_erp_px_cat_g1v2', @t_start, @t_end,
        TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end) / 1000000);

-- ============================================================
-- Final reporting: per-table + total duration
-- ============================================================
SET @job_end := NOW(6);

SELECT
  table_name,
  started_at,
  ended_at,
  ROUND(duration_sec, 3) AS duration_sec
FROM etl_load_metrics
ORDER BY started_at;

SELECT
  'TOTAL' AS table_name,
  @job_start AS started_at,
  @job_end   AS ended_at,
  ROUND(TIMESTAMPDIFF(MICROSECOND, @job_start, @job_end) / 1000000, 3) AS duration_sec;
