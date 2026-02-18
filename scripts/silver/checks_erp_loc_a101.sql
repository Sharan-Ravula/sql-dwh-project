-- bronze.erp_loc_a101
-- Quality Checks

-- Solution:
SELECT
    replace(cid, '-', '') as cid, -- Invalid Values
    case
        when upper(trim(cntry)) in ('US', 'USA') then 'United States'
        when upper(trim(cntry)) = 'DE' then 'Germany'
        when upper(trim(cntry)) = '' or cntry is NULL then 'n/a'
        else trim(cntry)
    end as cntry -- Data Normalization and handle missing values
from bronze.erp_loc_a101;

-- to find the difference between primary key cid of erp_loc_a101 and primary key cst_key of crm_cust_info
-- cst_key
SELECT
    cst_key
from Silver.crm_cust_info;

-- fixing the cid and checking if there are any unmatched primary key
-- cid
Select
    replace(cid, '-', '') as cid
from bronze.erp_loc_a101
where replace(cid, '-', '') not in (SELECT cst_key from Silver.crm_cust_info);

-- to check how many distinct values are there in the column cntry
-- cntry
Select DISTINCT
    cntry
from bronze.erp_loc_a101
Order by cntry;

-- compare 
Select DISTINCT
    cntry
from silver.erp_loc_a101
Order by cntry;

-- fixing it and comparing with the old one
Select distinct
    cntry,
    case
        when upper(trim(cntry)) in ('US', 'USA') then 'United States'
        when upper(trim(cntry)) = 'DE' then 'Germany'
        when upper(trim(cntry)) = '' or cntry is NULL then 'n/a'
        else trim(cntry)
    end as new_cntry
from bronze.erp_loc_a101
Order by cntry;

-- final check
SELECT
    *
from silver.erp_loc_a101;