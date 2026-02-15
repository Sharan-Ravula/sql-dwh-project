/*
I cannot create a database as I am using azure cloud server and database to connect to the sql engine using vscode extension SQL Server (mssql), SQL Database Projects, SQL Bindings, and Data Workspace by Microsoft
The Below Script helps us identify the files by creation date, here I used month(create_date) = 2, as I uploaded the files in the database in February.
I have also used create schema code to create schema for my dwh project.
*/

-- select * from sys.tables;

-- select 
--     *
-- from sys.tables
-- where month(create_date) = 2
-- ORDER BY create_date desc;

create schema bronze;
GO

create schema silver;
GO

create schema gold;
go
