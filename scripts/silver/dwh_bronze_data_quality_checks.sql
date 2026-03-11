/*
=======================================================================================
dwh_silver_data_quality_checks.sql (MySQL) - Data Warehousing Project
=======================================================================================

Purpose:
  This script contains data quality checks, profiling queries, and business-rule
  validations performed on the Bronze layer before loading cleaned/transformed
  data into the Silver layer.

How to use:
  - Run sections as needed during development/debugging.
  - Most "Expectation: No result" queries should return 0 rows.
  - "Solution" queries show the transformation logic used later in Silver loads.

Scope:
  - CRM: Bronze layer Customers, Products, Sales Details
  - ERP: Bronze layer Customer (AZ12), Location (A101), Product Category (PX_CAT_G1V2)
=========================================================================================
*/

USE dwh;

-- =============================================================================
-- CRM | bronze_crm_cust_info
-- =============================================================================

-- Check for NULLs or duplicate primary keys
-- Expectation: No result
SELECT
  cst_id,
  COUNT(*) AS has_duplicates
FROM bronze_crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Solution: Keep latest record per customer
SELECT *
FROM (
  SELECT
    *,
    RANK() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_test
  FROM bronze_crm_cust_info
) AS t
WHERE t.flag_test = 1
  AND cst_id != 0;

-- Check for unwanted spaces in first/last name
-- Expectation: No results
SELECT cst_firstname
FROM bronze_crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze_crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Solution: Trim names (shown with dedupe logic)
SELECT
  cst_id,
  cst_key,
  TRIM(cst_firstname) AS cst_firstname,
  TRIM(cst_lastname)  AS cst_lastname,
  cst_marital_status,
  cst_gndr,
  cst_create_date
FROM (
  SELECT
    *,
    RANK() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_test
  FROM bronze_crm_cust_info
) AS t
WHERE t.flag_test = 1
  AND cst_id != 0;

-- Data standardization and consistency checks
SELECT DISTINCT cst_gndr
FROM bronze_crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM bronze_crm_cust_info;

-- Solution: Standardize marital status and gender (plus trimming/dedup)
SELECT
  cst_id,
  cst_key,
  TRIM(cst_firstname) AS cst_firstname,
  TRIM(cst_lastname)  AS cst_lastname,
  CASE UPPER(TRIM(cst_marital_status))
    WHEN 'M' THEN "Married"
    WHEN 'S' THEN "Single"
    ELSE 'n/a'
  END AS cst_marital_status,
  CASE UPPER(TRIM(cst_gndr))
    WHEN 'M' THEN "Male"
    WHEN 'F' THEN "Female"
    ELSE 'n/a'
  END AS cst_gndr,
  cst_create_date
FROM (
  SELECT
    *,
    RANK() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_test
  FROM bronze_crm_cust_info
) AS t
WHERE t.flag_test = 1
  AND cst_id != 0;

-- =============================================================================
-- CRM | bronze_crm_prd_info
-- =============================================================================

-- Check for nulls or duplicates in primary key
-- Expectation: No result
SELECT
  prd_id,
  COUNT(*) AS has_duplicates
FROM bronze_crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Create a new column where the first five characters were actually related to 
-- product category in the relatable structure to check with bronze_erp_px_cat_g1v2
SELECT
  prd_id,
  prd_key,
  REPLACE(SUBSTR(prd_key, 1, 5), "-", "_") AS prd_cat
FROM bronze_crm_prd_info
WHERE 'prd_cat' NOT IN (SELECT DISTINCT id FROM bronze_erp_px_cat_g1v2);


-- Create a new column where the starting from 7th character till end were actually related to 
-- prd_key in the relatable structure to check with bronze_crm_sales_details
SELECT
  prd_id,
  prd_key,
  SUBSTR(prd_key, 7, LENGTH(prd_key)) AS prd_key
FROM bronze_crm_prd_info
WHERE 'prd_key' NOT IN (SELECT DISTINCT sls_prd_key FROM bronze_crm_sales_details);


-- Check for unwanted spaces
-- Expectations: No results
SELECT prd_nm
FROM bronze_crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Numbers
-- Expectation No results
SELECT prd_id, prd_cost
FROM bronze_crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Data standardization and consistency checks
SELECT DISTINCT prd_line
FROM bronze_crm_prd_info;

-- Check invalid date orders / missing dates
-- Expectation: No results
SELECT prd_start_dt, prd_end_dt
FROM bronze_crm_prd_info
WHERE prd_start_dt IS NULL OR prd_end_dt IS NULL;

-- End date must not be less than start date
-- Expectation: No results
SELECT prd_start_dt, prd_end_dt
FROM bronze_crm_prd_info
WHERE prd_start_dt > prd_end_dt;


-- Solution: Derive the end date from the start date
-- END date of the current record comes from the start date of the 'next' record - 1.
-- Here it solve the issue of end date less then start date and also date overlapping in the prd_cat.

SELECT
  prd_id,
  prd_key,
  prd_nm,
  prd_start_dt,
  prd_end_dt,
  DATE_SUB(
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),
    INTERVAL 1 DAY
  ) AS prd_end_dt_test
FROM bronze_crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');


-- =============================================================================
-- CRM | bronze_crm_sales_details
-- =============================================================================

-- Check for nulls or duplicates in primary key
-- Expectation: No result
SELECT
  sls_ord_num,
  COUNT(*) AS has_duplicates
FROM bronze_crm_sales_details
GROUP BY sls_ord_num
HAVING COUNT(*) > 1 OR sls_ord_num IS NULL;


-- Check for unwanted spaces
-- Expectations: No results
SELECT sls_ord_num
FROM bronze_crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);


-- Check the integrity of prd_key and cust_id with sales_details table
-- Expectation 0 results
SELECT sls_ord_num, sls_prd_key
FROM bronze_crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver_crm_prd_info);

SELECT sls_ord_num, sls_cust_id
FROM bronze_crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver_crm_cust_info);

-- Check for invalid dates
-- Expectation: 0 results
SELECT sls_order_dt
FROM bronze_crm_sales_details
WHERE sls_order_dt <= 0;

SELECT sls_ship_dt
FROM bronze_crm_sales_details
WHERE sls_ship_dt <= 0;

SELECT sls_due_dt
FROM bronze_crm_sales_details
WHERE sls_due_dt <= 0;


-- Here in sls_order_dt there are zeros 
-- Solution: replace it will nulls
SELECT NULLIF(sls_order_dt, 0)
FROM bronze_crm_sales_details
WHERE sls_order_dt <= 0 OR LENGTH(sls_order_dt) != 8;

-- Check for date range
-- Expectation: 0 results
SELECT NULLIF(sls_order_dt, 0)
FROM bronze_crm_sales_details
WHERE sls_order_dt > 20500101 OR sls_order_dt < 19000101;

-- Now convert the data type from integer to a date for
-- sls_order_dt, sls_ship_dt, sls_due_dt
SELECT
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
  END AS sls_due_dt
FROM bronze_crm_sales_details;

-- Check for order date less then ship date and due date
-- Expectation: 0 results
SELECT sls_order_dt
FROM bronze_crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Business rules: No negatives, zeros, nulls in sales, quantiy and price 

-- Check for negatives, zeros, nulls in sales, quantiy and price
-- Expectation 0 results
SELECT sls_quantity
FROM bronze_crm_sales_details
WHERE sls_quantity <= 0 OR sls_quantity IS NULL;

SELECT sls_price
FROM bronze_crm_sales_details
WHERE sls_price <= 0 OR sls_price IS NULL;

SELECT sls_sales
FROM bronze_crm_sales_details
WHERE sls_sales <= 0 OR sls_sales IS NULL;

-- Business rules: Sales = Quantity * Price
-- Check this calculation
-- Expectation 0 results
SELECT sls_sales
FROM bronze_crm_sales_details
WHERE sls_sales != sls_price * sls_quantity;

-- Here to address this issue talk to business experts like what rule they want me to apply over here.
-- Some examples were,
-- Rule 1: If Sales is negative, zero or null. derive it using Quantity and Price.
-- Rule 2: If price is zero or null, calculate it using Sales and Quantity.
-- Rule 3: If price is negative, convert it to a positive value.

WITH base AS (
  SELECT
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
),
final as (
SELECT
    sls_sales,
    sls_quantity,
    sls_price,

    -- Step 2: recompute sales using the fixed price (Rule 1)
    CASE
      WHEN sls_sales IS NULL OR sls_sales <= 0
        OR sls_sales != sls_quantity * ABS(fixed_price)
      THEN sls_quantity * ABS(fixed_price)
      ELSE sls_sales
    END AS new_sls_sales,
    fixed_price AS new_sls_price
  FROM base
)
SELECT *
FROM final
WHERE sls_sales != sls_price * sls_quantity
   OR sls_quantity <= 0 OR sls_quantity IS NULL
   OR sls_price <= 0 OR sls_price IS NULL
   OR sls_sales <= 0 OR sls_sales IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;

-- =============================================================================
-- ERP | bronze_erp_cust_az12
-- =============================================================================

SELECT cid, bdate, gen
FROM bronze_erp_cust_az12;

-- Here cst_key from cust_info table need to be join with the cid
SELECT *
FROM silver_crm_cust_info;

-- Extra 3 characters were noticed in cid need to be removed to match with cst_key.
-- Expectation 0 result
SELECT
  CASE
    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
    ELSE cid
  END AS cid
FROM bronze_erp_cust_az12
WHERE CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
        ELSE cid
      END NOT IN (SELECT DISTINCT cst_key FROM silver_crm_cust_info);

-- Check of out of range bdates for customers
-- Expectation: 0 results
SELECT DISTINCT bdate
FROM bronze_erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > CURRENT_DATE();

-- Solution : replace extreme date with null
SELECT
  CASE
    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
    ELSE cid
  END AS cid,
  CASE
    WHEN bdate > CURRENT_DATE() THEN NULL
    ELSE bdate
  END AS bdate,
  gen
FROM bronze_erp_cust_az12
WHERE bdate > CURRENT_DATE();

-- Check for data consistency in the gen
SELECT DISTINCT gen
FROM bronze_erp_cust_az12;

-- Solution fix those values other then Male and Female
SELECT DISTINCT
  CASE
    WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
    WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
    ELSE 'n/a'
  END AS gen
FROM bronze_erp_cust_az12;

-- =============================================================================
-- ERP | bronze_erp_loc_a101
-- =============================================================================

SELECT cid, cntry
FROM bronze_erp_loc_a101;

-- Here cst_key from cust_info table need to be join with the cid
SELECT cst_key
FROM silver_crm_cust_info;

-- Extra '-' after 2 characters were noticed in cid need to be removed to match with cst_key.
-- Expectation 0 result
SELECT cid, REPLACE(cid, '-', '') AS cid
FROM bronze_erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (SELECT cst_key FROM silver_crm_cust_info);

-- Check for data consistency in the cntry
SELECT DISTINCT cntry
FROM bronze_erp_loc_a101;

-- Work on the transformation to make it consistent
SELECT DISTINCT
  CASE
    WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
    WHEN TRIM(cntry) IN ('DE') THEN 'Germany'
    WHEN cntry = '' OR cntry IS NULL THEN 'n/a'
    ELSE TRIM(cntry)
  END AS cntry
FROM bronze_erp_loc_a101;

-- =============================================================================
-- ERP | bronze_erp_px_cat_g1v2
-- =============================================================================

SELECT id, cat, subcat, maintenance
FROM bronze_erp_px_cat_g1v2;

-- Here prd_key from prd_info table need to be join with the id
SELECT prd_key
FROM silver_crm_prd_info
ORDER BY prd_key;

-- Check for unwanted spaces
-- Expectation 0 results
SELECT *
FROM bronze_erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance);

-- Check for data consistency

-- cat
SELECT DISTINCT cat
FROM bronze_erp_px_cat_g1v2;

-- subcat
SELECT DISTINCT subcat
FROM bronze_erp_px_cat_g1v2;

-- maintenance
SELECT DISTINCT maintenance
FROM bronze_erp_px_cat_g1v2;

-- From initial transformation we noticed that id matched with cat_id.
-- Expectation 0 result
SELECT id
FROM bronze_erp_px_cat_g1v2
WHERE id NOT IN (SELECT cat_id FROM silver_crm_prd_info);
