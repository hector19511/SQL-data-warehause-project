/*
PROCEDURE CREATION. THIS PROCEDURE LOADS THE DATA INTO THE TABLES AFTER TRANSFORMING IT
*/

EXEC silver.load_Silver;  -- EXECUTE THIS AT LAST, THIS EXECUTES THE PROCEDURE CREATED BELOW

GO

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @silver_start DATETIME, @silver_end DATETIME
    BEGIN TRY

        SET @silver_start = GETDATE();
        PRINT '========================='
        PRINT 'LOADING THE SILVER LAYER'
        PRINT '========================='

        PRINT '-------------------------'
        PRINT 'Loading CRM Tables'
        PRINT '-------------------------'

        SET @start_time = GETDATE();
        /*============================================================
        crm_cust_info
        Data Transformation and cleansing
        ============================================================*/
        PRINT '>>Truncating silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>>Inserting data into silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info(
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        -- Removing dupplicates from the ID column
        SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,--takes out unwanted spaces
        TRIM(cst_lastname) AS cst_lastname,
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'SINGLE'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'MARRIED'
            ELSE 'N/A'
        END cst_marital_status,

        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
            ELSE 'N/A'
        END cst_gndr,
        cst_create_date
        FROM (
        SELECT-- nested query 
        *,
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
        )t WHERE flag_last = 1;

        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------'

        SET @start_time = GETDATE();
        /*============================================================
        crm_prod_info
        Data Transformation and cleansing
        ============================================================*/
        PRINT '>>Truncating silver.crm_prod_info';
        TRUNCATE TABLE silver.crm_prod_info;

        PRINT '>>Inserting data into silver.crm_prod_info';
        INSERT INTO silver.crm_prod_info(
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
        REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
        SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
        prd_nm,
        ISNULL(prd_cost,0) AS prd_cost,
        CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road' 
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'N/A'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt ,
        CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt
        FROM bronze.crm_prod_info;

        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------'

        SET @start_time = GETDATE();
        /*============================================================
        crm_sales_details
        Data Transformation and cleansing
        ============================================================*/
        PRINT '>>Truncating silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>>Inserting data into silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details(
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
        SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
        END AS sls_order_dt,
        CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)
        END AS sls_ship_dt,
        CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)
        END AS sls_due_dt,
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales/NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
        FROM bronze.crm_sales_details;


        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------'

        PRINT '-------------------------'
        PRINT 'Loading ERP Tables'
        PRINT '-------------------------'

        SET @start_time = GETDATE();
        /*============================================================
        erp_cust_az12
        Data Transformation and cleansing
        ============================================================*/
        PRINT '>>Truncating silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
        SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
            ELSE cid 
        END AS cid,
        CASE WHEN bdate > GETDATE() THEN NULL
            ELSE bdate
        END AS bdate,
        CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------'

        SET @start_time = GETDATE();
        /*============================================================
        erp_loc_a101
        Data Transformation and cleansing
        ============================================================*/
        PRINT '>>Truncating silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>>Inserting data into silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101(cid,cntry)
        SELECT 
        REPLACE(cid,'-','') cid,                                      --handling invalid values
        CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'   --Data normalizaton
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------'

        SET @start_time = GETDATE();
        /*============================================================
        erp_px_cat_g1v2
        Data Transformation and cleansing
        ============================================================*/
        PRINT '>>Truncating silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>>Inserting data into silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 -- Nothing needed to be done
        (id,cat,subcat,maintenance)
        SELECT
        id,
        cat,
        subcat,
        maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------'

        SET @silver_end = GETDATE();
        PRINT '======================================'
        PRINT '>>Silver Stage Batch Load Duration: ' + CAST(DATEDIFF(second, @silver_start, @silver_end) AS NVARCHAR) + ' seconds';
        PRINT '======================================'

    END TRY
    BEGIN CATCH    -- handling errors
        PRINT '======================================'
        PRINT 'ERROR DURING LOAD OF SILVER LAYER'
        PRINT 'Error Message' + ERROR.MESSAGE();
        PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
        PRINT '======================================'
    END CATCH
END 

