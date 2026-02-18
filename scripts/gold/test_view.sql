-- customers
SELECT distinct
    gender
from gold.dim_customers

-- products
SELECT
    *
from gold.dim_products

-- sales
SELECT
    *
from gold.fact_sales

-- Foreign Key Integrity (Dimensions)
SELECT
    *
from gold.fact_sales as f
left join gold.dim_customers as c
on c.customer_key = f.customer_key
left join gold.dim_products as p
on p.product_key = f.product_key
where p.product_key is null