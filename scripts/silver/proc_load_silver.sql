/*
===============================================================================
Stored Procedure: Load silver Layer (Source -> silver)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema from bronze schema. 
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - Uses the `INSERT` command to load data from bronze tables to silver tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	
    BEGIN TRY
		SET @batch_start_time = GETDATE();

		PRINT '================================================';
		PRINT 'Loading silver Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';

		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';

		INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
        SELECT
            cst_id,
            cst_key,
            trim(cst_firstname) as cst_firstname,
            trim(cst_lastname) as cst_lastname,
            Case 
                when Upper(trim(cst_marital_status)) = 'S' then 'Single'
                when Upper(trim(cst_marital_status)) = 'M' then 'Married'
                Else 'N/A'
            END cst_marital_status,
            Case 
                when Upper(trim(cst_gndr)) = 'M' then 'Male'
                when Upper(trim(cst_gndr)) = 'F' then 'Female'
                Else 'N/A'
            END cst_gndr,
            cst_create_date
        from (
        SELECT
            *,
            ROW_NUMBER() over(Partition by cst_id ORDER BY cst_create_date DESC) as flag_last
        from bronze.crm_cust_info
        where cst_id is not null
        )t where flag_last = 1

		SET @end_time = GETDATE();
		
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';

		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';

		INSERT INTO silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
        Select 
            prd_id,
            REPLACE(SUBSTRING(REPLACE(prd_key, '-', '_'), 1, 5), 'CO_PE', 'CO_PD') as cat_id,
            SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
            prd_nm,
            Isnull(prd_cost, 0) as prd_cost,
            case
                when upper(trim(prd_line)) = 'R' then 'Road'
                when upper(trim(prd_line)) = 'M' then 'Mountain'
                when upper(trim(prd_line)) = 'T' then 'Touring'
                when upper(trim(prd_line)) = 'S' then 'Other Sales'
                else 'n/a'
            end prd_line,
            cast(prd_start_dt as date) as prd_start_dt,
            cast(lead(prd_start_dt) over(PARTITION BY prd_key ORDER BY prd_start_dt) as datetime) - 1 as prd_end_dt
        from bronze.crm_prd_info;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';

		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';

		INSERT INTO silver.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            Case
                when sls_order_dt = 0 or len(sls_order_dt) != 8 then NULL
                ELSE cast(cast(sls_order_dt as varchar) as date)
            end as sls_order_dt,
            Case
                when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then NULL
                ELSE cast(cast(sls_ship_dt as varchar) as date)
            end as sls_ship_dt,
            Case
                when sls_due_dt = 0 or len(sls_due_dt) != 8 then NULL
                ELSE cast(cast(sls_due_dt as varchar) as date)
            end as sls_due_dt,
            case
                when sls_sales is null or sls_sales <= 0 or cast(sls_sales as INT) != cast(sls_quantity as INT) * cast(ABS(sls_price) as INT) 
                    then cast(sls_quantity as INT) * cast(ABS(sls_price) as INT)
                else sls_sales
            end as sls_sales,
            sls_quantity,
            CASE
                when sls_price is null or sls_price <= 0 
                    then abs(cast(sls_sales as INT)) / abs(cast(NULLif(sls_quantity, 0) as INT))
                else abs(sls_price)
            end as sls_price
        from bronze.crm_sales_details;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';

		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';

		INSERT INTO silver.erp_loc_a101 (CID, CNTRY)
        SELECT
            replace(cid, '-', '') as cid,
            case
                when upper(trim(cntry)) in ('US', 'USA') then 'United States'
                when upper(trim(cntry)) = 'DE' then 'Germany'
                when upper(trim(cntry)) = '' or cntry is NULL then 'n/a'
                else trim(cntry)
            end as cntry
        from bronze.erp_loc_a101;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';

		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';

		INSERT INTO silver.erp_cust_az12 (CID, BDATE, GEN)
        SELECT
            case 
                when cid like 'NAS%' then substring(cid, 4, len(cid))
                else cid
            end as cid,
            CASE 
                WHEN bdate > getdate() then NULL
                else bdate
            End as bdate,
            CASE 
                when trim(upper(gen)) in ('F', 'FEMALE') then 'Female'
                when trim(upper(gen)) in ('M', 'MALE') then 'Male'
                else 'n/a'
            end as gen
        from bronze.erp_cust_az12;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';

		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

		INSERT INTO silver.erp_px_cat_g1v2 (ID, CAT, SUBCAT, MAINTENANCE)
        Select 
            id,
            cat,
            subcat,
            maintenance
        from bronze.erp_px_cat_g1v2;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='

	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING silver LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END