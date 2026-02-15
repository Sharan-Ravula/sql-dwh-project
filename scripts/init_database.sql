/*
I cannot create a database as I am using azure server and database to connect to the sql engine using vscode extension SQL Server (mssql), SQL Database Projects, SQL Bindings, and Data Workspace by Microsoft
The Below Script helps me identify the files by creation date. 
Here I used month(create_date) = 2, as I uploaded the files in the database in February.
I have also used 'create schema' code to create schema for my dwh project.

ETL Automation Using Azure Data Factory (ADF):

To convert from csv to sql, I first created a blob storage connected that to the ADF for the import of the source dataset (csv file), and then connected my server to the ADF aswell for the export of the sink dataset (sql table).
After Running the ADF pipeline the user has to select a folder and the automation will search every csv file in that folder and convert them to sql table.
I kinda messed up the first time I did it, as the automation took the entire file name as a single input [cust_info.csv] instead of cust_info, I fixed the automation and then changed it using TSQL commands 'EXEC sp_rename' below.

Also, the ETL will be using the dbo schema which is already present in the server, to move from dbo to your schema use 'Alter SCHEMA' command below
*/

select * from sys.tables;

select 
    *
from sys.tables
where month(create_date) = 2
ORDER BY create_date desc;

-- Schema Creation

create schema bronze;
GO

create schema silver;
GO

create schema gold;
go

-- Checking the Table Names

SELECT
    *
from CUST_AZ12;

SELECT
    *
from LOC_A101;

SELECT
    *
from PX_CAT_G1V2;

SELECT
    *
from [prd_info.csv];

SELECT
    *
from [sales_details.csv];

SELECT
    *
from [cust_info.csv];

-- Replacing the source_CRM folder file name '[filename.csv] to filename' and using name convention (snake_case) for both source_crm and source_erp files

-- Syntax: EXEC sp_rename 'OldTableName', 'NewTableName'

-- CRM

EXEC sp_rename '[prd_info.csv]', 'crm_prd_info';
go

EXEC sp_rename '[sales_details.csv]', 'crm_sales_details';
go

EXEC sp_rename '[cust_info.csv]', 'crm_cust_info';
go

-- ERP

EXEC sp_rename 'CUST_AZ12', 'erp_cust_az12';
go

EXEC sp_rename 'LOC_A101', 'erp_loc_a101';
go

EXEC sp_rename 'PX_CAT_G1V2', 'erp_px_cat_g1v2';
go

-- Checking the file names after exec sp_rename

SELECT
    *
from erp_cust_az12;

SELECT
    *
from erp_loc_a101;

SELECT
    *
from erp_px_cat_g1v2;

SELECT
    *
from crm_prd_info;

SELECT
    *
from crm_sales_details;

SELECT
    *
from crm_cust_info;

-- Since the schema used here will be dbo we have to alter

-- Syntax: ALTER SCHEMA [TargetSchema] TRANSFER [SourceSchema].[TableName]

-- CRM

ALTER SCHEMA bronze TRANSFER dbo.crm_prd_info;
ALTER SCHEMA bronze TRANSFER dbo.crm_sales_details;
ALTER SCHEMA bronze TRANSFER dbo.crm_cust_info;

-- ERP

ALTER SCHEMA bronze TRANSFER dbo.erp_cust_az12;
ALTER SCHEMA bronze TRANSFER dbo.erp_loc_a101;
ALTER SCHEMA bronze TRANSFER dbo.erp_px_cat_g1v2;

-- Additionally, as I am using this in the same database instead of creating a new one (as it maybe expensive using it in cloud)
-- I can just select the table by adding bronze.filename, simple!

SELECT
    *
from bronze.erp_cust_az12;

SELECT
    *
from bronze.erp_loc_a101;

SELECT
    *
from bronze.erp_px_cat_g1v2;

SELECT
    *
from bronze.crm_prd_info;

SELECT
    *
from bronze.crm_sales_details;

SELECT
    *
from bronze.crm_cust_info;
