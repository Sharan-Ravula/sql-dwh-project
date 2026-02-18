-- Bronze.crm_cust_info
-- Quality Checks

-- Solution:
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
)t where flag_last = 1;

-- Check for Nulls or Duplicates in Primary Key -- Expectation: No Result
-- to count how many duplicates are there in the primary key
-- cst_id
Select 
    cst_id,
    count(*)
from bronze.crm_cust_info
group by cst_id 
Having count(*) > 1 or cst_id is NULL;

-- compare
Select 
    cst_id,
    count(*)
from silver.crm_cust_info
group by cst_id 
Having count(*) > 1 or cst_id is NULL;

-- to check which row to choose from
SELECT
    *
from bronze.crm_cust_info
where cst_id = 29466;

-- check for unwanted spaces -- Expectation: No Results

-- cst_firstname
SELECT
    cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname);

-- to check and compare 
SELECT
    cst_firstname
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname);

-- cst_lastname
SELECT
    cst_lastname
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname);

-- to check and compare 
SELECT
    cst_lastname
from silver.crm_cust_info
where cst_lastname != trim(cst_lastname);

-- Data Standardization & Consistency
-- cst_gndr
SELECT distinct cst_gndr
from bronze.crm_cust_info;

-- to check and compare
SELECT distinct cst_gndr
from silver.crm_cust_info;

-- count check
Select count(*) from silver.crm_cust_info;
Select count(*) from bronze.crm_cust_info;

-- final check
SELECT *
from silver.crm_cust_info;