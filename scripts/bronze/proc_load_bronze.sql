CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @load_time_start DATETIME, @load_time_end DATETIME;
	BEGIN TRY
		SET @load_time_start = GETDATE();
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\MokshitBindal\Documents\SQL Tut\Data With Baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- FIRST ROW are column names
			FIELDTERMINATOR = ',',
			TABLOCK -- as data is added to table, it locks the table for safety and performance
		);
		SET @end_time= GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\MokshitBindal\Documents\SQL Tut\Data With Baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2, -- FIRST ROW are column names
			FIELDTERMINATOR = ',',
			TABLOCK -- as data is added to table, it locks the table for safety and performance
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\MokshitBindal\Documents\SQL Tut\Data With Baraa\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2, -- FIRST ROW are column names
			FIELDTERMINATOR = ',',
			TABLOCK -- as data is added to table, it locks the table for safety and performance
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\MokshitBindal\Documents\SQL Tut\Data With Baraa\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2, -- FIRST ROW are column names
			FIELDTERMINATOR = ',',
			TABLOCK -- as data is added to table, it locks the table for safety and performance
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\MokshitBindal\Documents\SQL Tut\Data With Baraa\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2, -- FIRST ROW are column names
			FIELDTERMINATOR = ',',
			TABLOCK -- as data is added to table, it locks the table for safety and performance
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\MokshitBindal\Documents\SQL Tut\Data With Baraa\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2, -- FIRST ROW are column names
			FIELDTERMINATOR = ',',
			TABLOCK -- as data is added to table, it locks the table for safety and performance
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		SET @load_time_end = GETDATE();
		PRINT '>> Bronze Load Duration: ' + CAST(DATEDIFF(second, @load_time_start, @load_time_end) AS NVARCHAR) + ' seconds';

	END TRY
	BEGIN CATCH
		PRINT 'ERROR WHILE LOADING BRONZE LAYER';
		PRINT 'Error message' + ERROR_MESSAGE();
		PRINT 'Error message' + CAST (ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'Error message' + CAST (ERROR_STATE() AS NVARCHAR);
	END CATCH
END


EXEC bronze.load_bronze;



SELECT COUNT(*) FROM bronze.crm_cust_info;
SELECT COUNT(*) FROM bronze.crm_prd_info;
SELECT COUNT(*) FROM bronze.crm_sales_details;

SELECT * FROM bronze.crm_cust_info;
SELECT * FROM bronze.crm_prd_info;
SELECT * FROM bronze.crm_sales_details; 

SELECT COUNT(*) FROM bronze.erp_cust_az12;
SELECT COUNT(*) FROM bronze.erp_loc_a101;
SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;

SELECT * FROM bronze.erp_cust_az12;
SELECT * FROM bronze.erp_loc_a101;
SELECT * FROM bronze.erp_px_cat_g1v2;
