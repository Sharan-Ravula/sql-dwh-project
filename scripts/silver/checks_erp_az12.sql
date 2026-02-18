-- bronze.erp_az12
-- Quality Checks

-- Solution:
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

-- to connect the tables of crm_cust_info and erp_cust_az12, we found that cid and cst_key can be linked as primary keys
-- Removing "NAS" from cid to connect both columns
-- cid
SELECT
    case 
        when cid like 'NAS%' then substring(cid, 4, len(cid))
        else cid
    end as cid
from bronze.erp_cust_az12;

-- to find if there are any unmatching date between crm_cust_info and erp_cust_az12
SELECT
    case 
        when cid like 'NAS%' then substring(cid, 4, len(cid))
        else cid
    end as cid
from bronze.erp_cust_az12
where case 
        when cid like 'NAS%' then substring(cid, 4, len(cid))
        else cid
      end not in (Select cst_key from silver.crm_cust_info);

-- to check the range of bdate and to also check if any bdate is greater than the current date
-- bdate
Select 
    CASE 
        WHEN bdate > getdate() then NULL
        else bdate
    End as bdate
from bronze.erp_cust_az12;

-- to check how many distinct values are present in the column gender
-- gen
Select DISTINCT
    gen,
    CASE 
        when trim(upper(gen)) = 'F' then 'FEMALE'
        when trim(upper(gen)) = 'M' then 'MALE'
        when trim(upper(gen))= '' then 'n/a' 
        when gen is NULL then 'n/a'
        else trim(upper(gen))
    end as gen
from bronze.erp_cust_az12;

-- another way (better way)
Select DISTINCT
    gen,
    CASE 
        when trim(upper(gen)) in ('F', 'FEMALE') then 'Female'
        when trim(upper(gen)) in ('M', 'MALE') then 'Male'
        else 'n/a'
    end as gen
from bronze.erp_cust_az12;

-- final check
Select
    *
from silver.erp_cust_az12;