-- bronze.crm_prd_info
-- Quality Checks

-- Solution:
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

-- check duplicates in the primary key
-- prd_id
Select 
    prd_id,
    count(*)
from bronze.crm_prd_info
group by prd_id 
Having count(*) > 1 or prd_id is NULL;

-- check
Select 
    prd_id,
    count(*)
from silver.crm_prd_info
group by prd_id 
Having count(*) > 1 or prd_id is NULL;

-- to check if primary key is there in the other tables
-- id
select distinct id from bronze.erp_px_cat_g1v2;
select sls_prd_key from bronze.crm_sales_details;

-- check for unwanted spaces -- Expectation: No Results
-- prd_nm
SELECT
    prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm);

-- check
SELECT
    prd_nm
from silver.crm_prd_info
where prd_nm != trim(prd_nm);

-- Check for nulls or negative numbers -- Expectation: No results
-- prd_cost
select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null;

-- check
select prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null;

-- Data Standardization & Consistency
-- prd_line
Select distinct prd_line
from bronze.crm_prd_info;

-- check
Select distinct prd_line
from silver.crm_prd_info;

-- Check for Invalid Date Orders
-- prd_end_dt & prd_start_dt
Select *
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt;
 
-- check
Select *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt;

-- fixing the date values
SELECT
    prd_id,
    prd_key,
    prd_nm,
    prd_start_dt,
    prd_end_dt,
    cast(lead(prd_start_dt) over(PARTITION BY prd_key ORDER BY prd_start_dt) as datetime) - 1 as prd_end_dt_test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

-- final check
select *
from silver.crm_prd_info;