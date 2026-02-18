-- bronze.crm_sales_details
-- Quality Checks

-- Solution:
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    Case
        when sls_order_dt = 0 or len(sls_order_dt) != 8 then NULL -- Invalid Data
        ELSE cast(cast(sls_order_dt as varchar) as date) -- Data type casting
    end as sls_order_dt,
    Case
        when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then NULL
        ELSE cast(cast(sls_ship_dt as varchar) as date)
    end as sls_ship_dt,
    Case
        when sls_due_dt = 0 or len(sls_due_dt) != 8 then NULL
        ELSE cast(cast(sls_due_dt as varchar) as date)
    end as sls_due_dt,
    case
        when sls_sales is null or sls_sales <= 0 or cast(sls_sales as INT) != cast(sls_quantity as INT) * cast(ABS(sls_price) as INT) -- Handling missing data
            then cast(sls_quantity as INT) * cast(ABS(sls_price) as INT)
        else sls_sales
    end as sls_sales,
    CASE
        when sls_price is null or sls_price <= 0 -- Invalid Data
            then abs(cast(sls_sales as INT)) / abs(cast(NULLif(sls_quantity, 0) as INT))
        else sls_price
    end as sls_price,
    sls_quantity
from bronze.crm_sales_details

-- to check unwanted spaces
-- sls_ord_num
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num);

-- to check if the product key matches in the other tables for joining
-- sls_prd_key and sls_cust_id
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
from bronze.crm_sales_details
where sls_prd_key NOT IN (Select prd_key from silver.crm_prd_info)
and sls_cust_id NOT IN (Select cst_id from silver.crm_cust_info);

-- to convert any 0 to null 
-- to check if the sls_order_dt length is less than 8
-- to check if the sls_order_dt is less than 1900's and more than 2050's
-- sls_order_dt
SELECT
    NULLIF(sls_order_dt, 0) as sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 
or len(sls_order_dt) < 8
or sls_order_dt > 20500101
or sls_order_dt < 19000101;

-- fixing all the problems which we encountered above by using case 
SELECT
    Case
        when sls_order_dt = 0 or len(sls_order_dt) != 8 then NULL
        ELSE cast(cast(sls_order_dt as varchar) as date)
    end as sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 
or len(sls_order_dt) < 8
or sls_order_dt > 20500101
or sls_order_dt < 19000101;

-- to check if the same stuff for the ship date
-- sls_ship_dt
SELECT
    sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt <= 0 
or len(sls_ship_dt) < 8
or sls_ship_dt > 20500101
or sls_ship_dt < 19000101;

-- to check if the same stuff for the due date
-- sls_due_dt
SELECT
    sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0 
or len(sls_due_dt) < 8
or sls_due_dt > 20500101
or sls_due_dt < 19000101;

-- to check if the order date is less than ship date or due date
SELECT
    *
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- compare and check
SELECT
    *
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- to check data consistency: between sales, quantity and price
-- Sales = Quantity * Price
-- values must not be null, zero or negative
-- sls_quantity, sls_sales, sls_price
SELECT
    sls_quantity,
    case
        when sls_sales is null or sls_sales <= 0 or cast(sls_sales as INT) != cast(sls_quantity as INT) * cast(ABS(sls_price) as INT) 
            then cast(sls_quantity as INT) * cast(ABS(sls_price) as INT)
        else sls_sales
    end as sls_sales,
    CASE
        when sls_price is null or sls_price <= 0 
            then abs(cast(sls_sales as INT)) / abs(cast(NULLif(sls_quantity, 0) as INT))
        else sls_price
    end as sls_price
from bronze.crm_sales_details
where cast(sls_sales as INT) != cast(sls_quantity as INT) * cast(sls_price as INT)
OR sls_sales is null or sls_quantity is null or sls_price is null
OR sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
Order by sls_sales, sls_quantity, sls_price;

-- compare and check
SELECT
    sls_sales,
    sls_quantity,
    sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
OR sls_sales is null or sls_quantity is null or sls_price is null
OR sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
Order by sls_sales, sls_quantity, sls_price;

-- final check
select * from silver.crm_sales_details;