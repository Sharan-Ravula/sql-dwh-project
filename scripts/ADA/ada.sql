-- Advance Data Analystics

-- Step 7: Change Over time (Trends)
-- Analyze how a measure evolves over time

-- ∑[Measure] By [Date Dimension]
-- Total Sales by Year
-- Average Cost by Month

-- Usually we target the fact table

-- Analyze Sales Performance over time
SELECT
    DATETRUNC(month, order_date) as order_date,
    sum(sales_amount) as total_sales,
    count(distinct customer_key) as total_customers,
    sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by DATETRUNC(month, order_date)
order by DATETRUNC(month, order_date) ASC
;

-- Step: 8 Cumulative Analysis
-- Aggregate the data progressively over time
-- Helps to understand if our business is growing or declining over time

-- ∑[Cumulative Measure] by [Date Dimension]
-- Running Total Sales by Year
-- Moving Average of Sales by Month

-- Calculate the total sales per month
-- and the running total of sales over time
Select 
    order_date,
    total_sales,
    sum(total_sales) over (partition by order_date order by order_date) as running_total_sales
from (
Select 
    DATETRUNC(month, order_date) as order_date,
    sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by DATETRUNC(month, order_date)
)t
;

-- year 
Select 
    order_date,
    total_sales,
    sum(total_sales) over (order by order_date) as running_total_sales
from (
Select 
    DATETRUNC(year, order_date) as order_date,
    sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by DATETRUNC(year, order_date)
)t
;

-- Moving average
Select 
    order_date,
    total_sales,
    sum(total_sales) over (order by order_date) as running_total_sales,
    avg(avg_price) over (order by order_date) as moving_average_price
from (
Select 
    DATETRUNC(year, order_date) as order_date,
    sum(sales_amount) as total_sales,
    avg(price) as avg_price
from gold.fact_sales
where order_date is not null
group by DATETRUNC(year, order_date)
)t
;

-- Step: 9 Performance Analysis
-- Comparing the current value to a target value

-- Current[Measure] - Target[Measure]
-- Current Sales - Average Sales
-- Current Year Sales - Previous Year Sales
-- Current Sales - Lowest Sales

-- Analyze the yearly performance of products 
-- by comparing each product's sales to both 
-- its average sales performance and the previous year's sales.
with cte_yearly_product_sales
as (
Select 
    year(f.order_date) as order_year,
    p.product_name,
    sum(f.sales_amount) as current_sales
from gold.fact_sales as f
left join gold.dim_products as p
on f.product_key = p.product_key
where f.order_date is not null
group by 
    year(f.order_date),
    p.product_name
)

Select 
    order_year,
    product_name,
    current_sales,
    avg(current_sales) over(partition by product_name) as avg_sales,
    current_sales - avg(current_sales) over(partition by product_name) as diff_avg,
    case 
        when current_sales - avg(current_sales) over(partition by product_name) > 0 then 'Above Avg'
        when current_sales - avg(current_sales) over(partition by product_name) < 0 then 'Below Avg'
        else 'Avg'
    end as avg_change,
    -- year over year analysis
    lag(current_sales) over(partition by product_name order by order_year) as previous_year_sales,
    current_sales - lag(current_sales) over(partition by product_name order by order_year) as diff_previous_year,
    case 
        when current_sales - lag(current_sales) over(partition by product_name order by order_year) > 0 then 'Increase'
        when current_sales - lag(current_sales) over(partition by product_name order by order_year) < 0 then 'Decrease'
        else 'No change'
    end as previous_year_change
from cte_yearly_product_sales
order by product_name, order_year
;

-- Step: 10 Part to whole Analysis
-- Analyze how an individual part is performing compared to the overall,
-- allowing us to understand which category has the greatest impact on the business.

-- ([Measure]/Total[Measure]) * 100 by [Dimension]
-- (Sales/ Total Sales) * 100 by Category
-- (Quantity/Total Quantity) * 100 by Country

-- Which categories contributes the most to overall sales
with cte_category_sales as (
SELECT
    category,
    SUM(sales_amount) as total_sales
from gold.fact_sales as f
left join gold.dim_products as p
on p.product_key = f.product_key
group by category
)

Select 
    category,
    total_sales,
    SUM(total_sales) over() as overall_sales,
    concat(round(cast(total_sales as float) / SUM(total_sales) over()  * 100, 2), '%') as percentage_of_total
from cte_category_sales
;

-- Step: 11 Data Segmentation
-- Group the data based on a specific range
-- Helps us understand the correlation between two measures

-- [Measure] by [Measure]
-- Total Products by Sales Range
-- Total Customers by Age

-- Segment products into cost ranges and count how many product fall into each segment
with cte_products_segments as (
Select 
    product_key,
    product_name,
    cost,
    case 
        when cost < 100 then 'Below 100'
        when cost between 100 and 500 then '100-500'
        when cost between 500 and 1000 then '500-1000'
        else 'Above 1000'
    end cost_range
from gold.dim_products
)

SELECT
    cost_range,
    count(product_key) as total_products
from cte_products_segments
group by cost_range
order by total_products desc
;

-- Group customers into three segments based on their spending behavior
-- VIP: atleast 12 months of history and spending more than 5000$
-- Regular: atleast 12 months of history and spending $5000 or less
-- New: Lifespan less than 12 months
-- And find the total number of customers by each group
with cte_customer_spending as (
Select 
    c.customer_key,
    sum(f.sales_amount) as total_spending,
    min(order_date) as first_order,
    max(order_date) as last_order,
    datediff(month, min(order_date), max(order_date)) as lifespan
from gold.fact_sales as f
left join gold.dim_customers as c
on f.customer_key = c.customer_key
group by c.customer_key
)


Select 
    customer_segment,
    count(customer_key) as total_customers
from (
Select 
    customer_key,
    case
        when lifespan >= 12 and total_spending > 5000 then 'VIP'
        when lifespan >= 12 and total_spending <= 5000 then 'Regular'
        else 'New'
    end as customer_segment
from cte_customer_spending
)t
group by customer_segment
order by total_customers desc
;
