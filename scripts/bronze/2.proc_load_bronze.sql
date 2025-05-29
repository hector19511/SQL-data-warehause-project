/*=====================================================
Loading the csv file data into te databases
======================================================*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS -- PROCEDURE WILL ACT LIKE A FUNCTION IN PYTHION. It will execute everything when executing bronze.load_bronze
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @bronze_start DATETIME, @bronze_end DATETIME
    BEGIN TRY

        SET @bronze_start = GETDATE();
        PRINT '========================='
        PRINT 'LOADING THE BRONZE LAYER'
        PRINT '========================='

        PRINT '-------------------------'
        PRINT 'Loading CRM Tables'
        PRINT '-------------------------'

        SET @start_time = GETDATE();
        PRINT '>>TRUNCATING TABLE: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info; -- deleting everything before loading again

        PRINT '>>INSERTING DATA: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/data/source_crm/cust_info.csv'
        WITH (
            -- This skips the first row in the csv, since those are the column headers
            FIRSTROW = 2,
            -- Tells SQL that the delimitator is a coma
            FIELDTERMINATOR = ',',
            -- Rows end with newline
            ROWTERMINATOR = '\n',       
            -- Locks the entire table during loading
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------'

        SET @start_time = GETDATE();
        PRINT '>>TRUNCATING TABLE: bronze.crm_prod_info';
        TRUNCATE TABLE bronze.crm_prod_info; 

        PRINT '>>INSERTING DATA: bronze.crm_prod_info';
        BULK INSERT bronze.crm_prod_info
        FROM '/var/opt/mssql/data/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',       
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------'

        SET @start_time = GETDATE();
        PRINT '>>TRUNCATING TABLE: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details; 

        PRINT '>>INSERTING DATA: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/data/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',       
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------'

        PRINT '-------------------------'
        PRINT 'Loading ERP Tables'
        PRINT '-------------------------'

        SET @start_time = GETDATE();
        PRINT '>>TRUNCATING TABLE: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12; 

        PRINT '>>INSERTING DATA: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/data/source_erp/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',       
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------'

        SET @start_time = GETDATE();
        PRINT '>>TRUNCATING TABLE: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101; 

        PRINT '>>INSERTING DATA: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM '/var/opt/mssql/data/source_erp/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',       
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------'
        

        SET @start_time = GETDATE();
        PRINT '>>TRUNCATING TABLE: bronze.erp_px_cat_g1v';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2; 

        PRINT '>>INSERTING DATA: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/var/opt/mssql/data/source_erp/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',       
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------'

        SET @bronze_end = GETDATE();
        PRINT '======================================'
        PRINT '>>Bronze Stage Batch Load Duration: ' + CAST(DATEDIFF(second, @bronze_start, @bronze_end) AS NVARCHAR) + ' seconds';
        PRINT '======================================'

    END TRY
    BEGIN CATCH
        PRINT '======================================'
        PRINT 'ERROR DURING LOAD OF BRONZE LAYER'
        PRINT 'Error Message' + ERROR.MESSAGE();
        PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
        PRINT '======================================'
    END CATCH
END
GO

EXECUTE bronze.load_bronze;
