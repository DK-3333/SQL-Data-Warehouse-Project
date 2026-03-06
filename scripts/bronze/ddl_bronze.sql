/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================

Purpose:
  Bulk-load raw CSV files into Bronze tables (landing/raw layer).
  This script is intended to be run as a repeatable "daily load" job.

Notes:
  - Uses LOAD DATA LOCAL INFILE, so files are read from your desktop (client).
  - Ensure these are enabled:
      1) Server: local_infile = ON
      2) Workbench: Preferences -> SQL Editor -> Enable "LOAD DATA LOCAL INFILE"
      3) Connection: OPT_LOCAL_INFILE=1 (Connection -> Advanced -> Others)

WARNING:
  TRUNCATE TABLE deletes all rows in the target Bronze tables before reloading.
===============================================================================
*/

USE dwh;

-- ============================================================
-- ETL duration tracking (per-table + total)
-- ============================================================
DROP TEMPORARY TABLE IF EXISTS etl_load_metrics;
CREATE TEMPORARY TABLE etl_load_metrics (
  table_name      VARCHAR(150) NOT NULL,
  started_at      DATETIME(6)  NOT NULL,
  ended_at        DATETIME(6)  NOT NULL,
  duration_sec    DECIMAL(12,6) NOT NULL
);

SET @job_start := NOW(6);

-- ============================================================================
-- 1) CRM - Customers
-- ============================================================================
SET @t_start := NOW(6);

TRUNCATE TABLE bronze_crm_cust_info;

LOAD DATA LOCAL INFILE 'path/source_crm/cust_info.csv'
INTO TABLE bronze_crm_cust_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date);

SET @t_end := NOW(6);
INSERT INTO etl_load_metrics(table_name, started_at, ended_at, duration_sec)
VALUES ('bronze_crm_cust_info', @t_start, @t_end,
        TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end) / 1000000);

-- ============================================================================
-- 2) CRM - Products
-- ============================================================================
SET @t_start := NOW(6);
TRUNCATE TABLE bronze_crm_prd_info;

LOAD DATA LOCAL INFILE 'path/source_crm/prd_info.csv'
INTO TABLE bronze_crm_prd_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(prd_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt);

SET @t_end := NOW(6);
INSERT INTO etl_load_metrics(table_name, started_at, ended_at, duration_sec)
VALUES ('bronze_crm_prd_info', @t_start, @t_end,
        TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end) / 1000000);

-- ============================================================================
-- 3) CRM - Sales Details
-- ============================================================================
SET @t_start := NOW(6);
TRUNCATE TABLE bronze_crm_sales_details;

LOAD DATA LOCAL INFILE 'path/source_crm/sales_details.csv'
INTO TABLE bronze_crm_sales_details
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price);

SET @t_end := NOW(6);
INSERT INTO etl_load_metrics(table_name, started_at, ended_at, duration_sec)
VALUES ('bronze_crm_sales_details', @t_start, @t_end,
        TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end) / 1000000);

-- ============================================================================
-- 4) ERP - Customer AZ12
-- ============================================================================
SET @t_start := NOW(6);
TRUNCATE TABLE bronze_erp_cust_az12;

LOAD DATA LOCAL INFILE 'path/source_erp/CUST_AZ12.csv'
INTO TABLE bronze_erp_cust_az12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(CID,BDATE,GEN);

SET @t_end := NOW(6);
INSERT INTO etl_load_metrics(table_name, started_at, ended_at, duration_sec)
VALUES ('bronze_erp_cust_az12', @t_start, @t_end,
        TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end) / 1000000);

-- ============================================================================
-- 5) ERP - Location A101
-- ============================================================================
SET @t_start := NOW(6);
TRUNCATE TABLE bronze_erp_loc_a101;

LOAD DATA LOCAL INFILE 'path/source_erp/LOC_A101.csv'
INTO TABLE bronze_erp_loc_a101
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(CID,CNTRY);

SET @t_end := NOW(6);
INSERT INTO etl_load_metrics(table_name, started_at, ended_at, duration_sec)
VALUES ('bronze_erp_loc_a101', @t_start, @t_end,
        TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end) / 1000000);

-- ============================================================================
-- 6) ERP - Product Category G1V2
-- ============================================================================
SET @t_start := NOW(6);
TRUNCATE TABLE bronze_erp_px_cat_g1v2;

LOAD DATA LOCAL INFILE 'path/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze_erp_px_cat_g1v2
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(ID,CAT,SUBCAT,MAINTENANCE);

SET @t_end := NOW(6);
INSERT INTO etl_load_metrics(table_name, started_at, ended_at, duration_sec)
VALUES ('bronze_erp_px_cat_g1v2', @t_start, @t_end,
        TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end) / 1000000);

SET @job_end := NOW(6);

-- Per-table duration
SELECT
  table_name,
  ROUND(duration_sec, 3) AS duration_sec
FROM etl_load_metrics
ORDER BY started_at;

-- Total duration (all tables combined)
SELECT
  'TOTAL' AS table_name,
  ROUND(TIMESTAMPDIFF(MICROSECOND, @job_start, @job_end) / 1000000, 3) AS duration_sec;

-- ============================================================================
-- Final quick summary (all Bronze tables)
-- ============================================================================
SELECT 'bronze_crm_cust_info'      AS table_name, COUNT(*) AS row_count FROM bronze_crm_cust_info
UNION ALL
SELECT 'bronze_crm_prd_info',           COUNT(*) FROM bronze_crm_prd_info
UNION ALL
SELECT 'bronze_crm_sales_details',      COUNT(*) FROM bronze_crm_sales_details
UNION ALL
SELECT 'bronze_erp_cust_az12',          COUNT(*) FROM bronze_erp_cust_az12
UNION ALL
SELECT 'bronze_erp_loc_a101',           COUNT(*) FROM bronze_erp_loc_a101
UNION ALL
SELECT 'bronze_erp_px_cat_g1v2',        COUNT(*) FROM bronze_erp_px_cat_g1v2;

