-- bronze.erp_px_cat_g1v2
-- Quality Check

-- Solution:
Select 
    id,
    cat,
    subcat,
    Maintenance
from bronze.erp_px_cat_g1v2;

-- 
SELECT TOp 1000
    *
from silver.crm_prd_info;

-- to check the if theere are any unmatched primary key id in erp_px_cat_g1v2 and cat_id silver.crm_prd_info
Select 
    id
from bronze.erp_px_cat_g1v2
where id not in (Select cat_id from silver.crm_prd_info);

-- check for unwanted spaces
SELECT
    *
from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or Maintenance != trim(Maintenance);

-- data standardization & consistency
Select DISTINCT
    cat,
    subcat,
    maintenance
from bronze.erp_px_cat_g1v2;

-- final check
SELECT
    *
from silver.erp_px_cat_g1v2