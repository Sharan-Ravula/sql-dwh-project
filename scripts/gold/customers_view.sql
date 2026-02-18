-- joining all the tables which have the same primary key and then checking if there are any duplicates in the primary key
Select 
    cst_id, count(*) 
from (
SELECT
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_marital_status,
    ci.cst_gndr,
    ci.cst_create_date,
    ca.bdate,
    ca.gen,
    la.cntry
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 as la
on ci.cst_key = la.cid
)t group by cst_id
having count(*) > 1;

-- data integration, who is the master? if crm is the master, then crm is more accurate
SELECT Distinct
    ci.cst_gndr,
    ca.gen,
    Case 
        when ci.cst_gndr != 'n/a' then ci.cst_gndr
        else ca.gen
    end as new_gen
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 as la
on ci.cst_key = la.cid
Order by 1, 2

-- for the full table solution:
create view gold.dim_customers as
SELECT
    ROW_NUMBER() over(Order by cst_id) as customer_key, -- seragate key
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
    la.cntry as country,
    ci.cst_marital_status as marital_status,
    Case 
        when ci.cst_gndr != 'n/a' then ci.cst_gndr
        else ca.gen
    end as gender,
    ca.bdate as birth_date,
    ci.cst_create_date as create_date
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 as la
on ci.cst_key = la.cid;