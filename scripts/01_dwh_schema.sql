/*
===============================================================================
01_dwh_schema.sql (MySQL) - Data Warehousing Project
===============================================================================

Purpose:
  1) Create a fresh 'dwh' database (drops if exists).
  2) Create Bronze tables (raw landing tables).
  3) Create Silver tables (cleaned/standardized tables with load timestamp).

Design note:
  MySQL does not support nested schemas like SQL Server in the same way.
  Bronze/Silver/Gold are represented using table prefixes:
    - bronze_*
    - silver_*
    - gold_* (views created separately)

WARNING:
  This script DROPS the entire 'dwh' database if it exists.
  All existing tables/data will be permanently deleted.
===============================================================================
*/


-- =============================================================================
-- 0) Database Setup
-- =============================================================================
DROP DATABASE IF EXISTS dwh;
CREATE DATABASE dwh;
USE dwh;

-- =============================================================================
-- 1) Bronze Layer: Table Definitions (Raw Landing)
-- =============================================================================
-- Source: CRM

DROP TABLE IF EXISTS bronze_crm_cust_info;
CREATE TABLE bronze_crm_cust_info (
  cst_id             INT,
  cst_key            VARCHAR(50),
  cst_firstname      VARCHAR(50),
  cst_lastname       VARCHAR(50),
  cst_marital_status VARCHAR(50),
  cst_gndr           VARCHAR(50),
  cst_create_date    DATE
);

DROP TABLE IF EXISTS bronze_crm_prd_info;
CREATE TABLE bronze_crm_prd_info (
  prd_id       INT,
  prd_key      VARCHAR(50),
  prd_nm       VARCHAR(50),
  prd_cost     INT,
  prd_line     VARCHAR(50),
  prd_start_dt DATE,
  prd_end_dt   DATE
);

DROP TABLE IF EXISTS bronze_crm_sales_details;
CREATE TABLE bronze_crm_sales_details (
  sls_ord_num  VARCHAR(50),
  sls_prd_key  VARCHAR(50),
  sls_cust_id  INT,
  sls_order_dt INT,
  sls_ship_dt  INT,
  sls_due_dt   INT,
  sls_sales    INT,
  sls_quantity INT,
  sls_price    INT
);

-- Source: ERP

DROP TABLE IF EXISTS bronze_erp_cust_az12;
CREATE TABLE bronze_erp_cust_az12 (
  cid   VARCHAR(50),
  bdate DATE,
  gen   VARCHAR(50)
);

DROP TABLE IF EXISTS bronze_erp_loc_a101;
CREATE TABLE bronze_erp_loc_a101 (
  cid   VARCHAR(50),
  cntry VARCHAR(50)
);

DROP TABLE IF EXISTS bronze_erp_px_cat_g1v2;
CREATE TABLE bronze_erp_px_cat_g1v2 (
  id          VARCHAR(50),
  cat         VARCHAR(50),
  subcat      VARCHAR(50),
  maintenance VARCHAR(50)
);

-- Note:
-- Use dwh_load_bronze.sql inside bronze folder to ingest CSV files into Bronze tables.


-- =============================================================================
-- 3) Silver Layer: Table Definitions (Cleaned/Standardized)
-- =============================================================================

-- Source: CRM

DROP TABLE IF EXISTS silver_crm_cust_info;
CREATE TABLE silver_crm_cust_info (
  cst_id             INT,
  cst_key            VARCHAR(50),
  cst_firstname      VARCHAR(50),
  cst_lastname       VARCHAR(50),
  cst_marital_status VARCHAR(50),
  cst_gndr           VARCHAR(50),
  cst_create_date    DATE,
  dwh_create_date    DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
);

DROP TABLE IF EXISTS silver_crm_prd_info;
CREATE TABLE silver_crm_prd_info (
  prd_id          INT,
  cat_id          VARCHAR(50),
  prd_key         VARCHAR(50),
  prd_nm          VARCHAR(50),
  prd_cost        INT,
  prd_line        VARCHAR(50),
  prd_start_dt    DATE,
  prd_end_dt      DATE,
  dwh_create_date DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
);

DROP TABLE IF EXISTS silver_crm_sales_details;
CREATE TABLE silver_crm_sales_details (
  sls_ord_num     VARCHAR(50),
  sls_prd_key     VARCHAR(50),
  sls_cust_id     INT,
  sls_order_dt    DATE,
  sls_ship_dt     DATE,
  sls_due_dt      DATE,
  sls_sales       INT,
  sls_quantity    INT,
  sls_price       INT,
  dwh_create_date DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
);

-- Source: ERP

DROP TABLE IF EXISTS silver_erp_cust_az12;
CREATE TABLE silver_erp_cust_az12 (
  cid             VARCHAR(50),
  bdate           DATE,
  gen             VARCHAR(50),
  dwh_create_date DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
);

DROP TABLE IF EXISTS silver_erp_loc_a101;
CREATE TABLE silver_erp_loc_a101 (
  cid             VARCHAR(50),
  cntry           VARCHAR(50),
  dwh_create_date DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
);

DROP TABLE IF EXISTS silver_erp_px_cat_g1v2;
CREATE TABLE silver_erp_px_cat_g1v2 (
  id              VARCHAR(50),
  cat             VARCHAR(50),
  subcat          VARCHAR(50),
  maintenance     VARCHAR(50),
  dwh_create_date DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
);

-- Note:
-- Use dwh_load_silver.sql inside silver folder to populate Silver tables from Bronze.
