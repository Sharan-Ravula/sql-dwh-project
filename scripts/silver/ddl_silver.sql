/*
===============================================================================
DDL Script: Create silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE [silver].[crm_cust_info](
	[cst_id] [INT] NULL,
	[cst_key] [nvarchar](50) NULL,
	[cst_firstname] [nvarchar](50) NULL,
	[cst_lastname] [nvarchar](50) NULL,
	[cst_marital_status] [nvarchar](50) NULL,
	[cst_gndr] [nvarchar](50) NULL,
	[cst_create_date] [Date] NULL,
    [dwh_create_date] [Datetime2] DEFAULT GETDATE()
) ON [PRIMARY] 
GO

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE [silver].[crm_prd_info](
	[prd_id] [INT] NULL,
    [cat_id] [nvarchar](50) NULL,
	[prd_key] [nvarchar](50) NULL,
	[prd_nm] [nvarchar](50) NULL,
	[prd_cost] [INT] NULL,
	[prd_line] [nvarchar](50) NULL,
	[prd_start_dt] [Date] NULL,
	[prd_end_dt] [Date] NULL,
    [dwh_create_date] [Datetime2] DEFAULT GETDATE()
) ON [PRIMARY] 
GO

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE [silver].[crm_sales_details](
	[sls_ord_num] [nvarchar](50) NULL,
	[sls_prd_key] [nvarchar](50) NULL,
	[sls_cust_id] [INT] NULL,
	[sls_order_dt] [Date] NULL,
	[sls_ship_dt] [Date] NULL,
	[sls_due_dt] [Date] NULL,
	[sls_sales] [INT] NULL,
	[sls_quantity] [INT] NULL,
	[sls_price] [INT] NULL,
    [dwh_create_date] [Datetime2] DEFAULT GETDATE()
) ON [PRIMARY]
GO

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE [silver].[erp_loc_a101](
	[CID] [nvarchar](50) NULL,
	[CNTRY] [nvarchar](50) NULL,
    [dwh_create_date] [Datetime2] DEFAULT GETDATE()
) ON [PRIMARY]
GO

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE [silver].[erp_cust_az12](
	[CID] [nvarchar](50) NULL,
	[BDATE] [Date] NULL,
	[GEN] [nvarchar](50) NULL,
    [dwh_create_date] [Datetime2] DEFAULT GETDATE()
) ON [PRIMARY]
GO

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE [silver].[erp_px_cat_g1v2](
	[ID] [nvarchar](50) NULL,
	[CAT] [nvarchar](50) NULL,
	[SUBCAT] [nvarchar](50) NULL,
	[MAINTENANCE] [nvarchar](50) NULL,
    [dwh_create_date] [Datetime2] DEFAULT GETDATE()
) ON [PRIMARY]
GO