create database superstore;
use superstore;

CREATE TABLE store (
    row_id INT PRIMARY KEY,
    order_id VARCHAR(30),
    order_date varchar(20),
    ship_date varchar(20),
    ship_mode VARCHAR(30),
    customer_id VARCHAR(20),
    customer_name VARCHAR(100),
    segment VARCHAR(30),
    country VARCHAR(40),
    city VARCHAR(40),
    state VARCHAR(40),
    postal_code VARCHAR(20),
    region VARCHAR(20),
    product_id VARCHAR(40),
    category VARCHAR(40),
    sub_category VARCHAR(40),
    product_name VARCHAR(250),
    sales DECIMAL(10,4),
    quantity INT,
    discount DECIMAL(5,4),
    profit DECIMAL(10,4)
);

select * from store;

SHOW VARIABLES LIKE 'local_infile';

LOAD DATA LOCAL INFILE '/home/naiduu/Desktop/all/superstore/superstore.csv'
INTO TABLE store
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

# after loading the dataset to store table: 

select * from store;

-- make a copy of original raw data

drop table store_copy;

create table store_copy
like store;

insert store_copy
select * from store
order by row_id asc;

select * from store_copy;

-- check for duplicates
with duplicate_cte as
(
	select *,
	row_number() over(
		partition by row_id, order_id, order_date, ship_date, ship_mode, customer_id,
		customer_name, segment, country, city, state, postal_code, region, product_id,
		category, sub_category, product_name, sales, quantity, discount, profit

	)
	as row_num 
	from store_copy 
)
select * from duplicate_cte
where row_num>1;
-- no duplicates



-- check for null data
SELECT * FROM store_copy
WHERE row_id IS NULL 
   OR order_id IS NULL 
   OR order_date IS NULL 
   OR ship_date IS NULL 
   OR ship_mode IS NULL 
   OR customer_id IS NULL 
   OR customer_name IS NULL 
   OR segment IS NULL 
   OR country IS NULL 
   OR city IS NULL 
   OR state IS NULL 
   OR postal_code IS NULL 
   OR region IS NULL 
   OR product_id IS NULL 
   OR category IS NULL 
   OR sub_category IS NULL 
   OR product_name IS NULL 
   OR sales IS NULL 
   OR quantity IS NULL 
   OR discount IS NULL 
   OR profit IS NULL;


-- standardize the data columns
-- date columns are varchar; not date

select order_date, ship_date
from store_copy;

select order_date,
str_to_date(order_date, '%m/%d/%Y')
from store_copy;

update store_copy
set order_date = str_to_date(order_date, '%m/%d/%Y');

select ship_date,
str_to_date(ship_date, '%m/%d/%Y')
from store_copy;

update store_copy
set ship_date = str_to_date(ship_date, '%m/%d/%Y');

select order_date, ship_date
from store_copy;

-- check for unwanted spacings and trim
select distinct state , trim(state)
from store_copy;

-- check if any issue in postal code
select distinct postal_code, length(postal_code) as len
from store_copy
order by len;




-- ANALYSIS

select * from store_copy;

-- first and last order and shipping date
select min(order_date), max(order_date), min(ship_date), max(ship_date)
from store_copy;

-- maximum orders with same order_id
select order_id, customer_name, count(*) as no_of_order
from store_copy
group by order_id, customer_name
order by no_of_order desc
limit 50;

-- top 50 customers getting the most discounts
select customer_name, discount
from store_copy
order by 2 desc
limit 50;

-- what segment produces the most profit
select segment, sum(profit) as total_profit
from store_copy
group by segment
order by 2 desc;

-- most profitable shipping method
select ship_mode, sum(profit) as total_profit
from store_copy
group by 1 order by 2 desc;

-- customers with most sales
select customer_name, count(*) total_sales
from store_copy
group by 1
order by 2 desc;

-- what category is most demandable 
select category, sub_category,
sum(quantity) as total_quantity
from store_copy
group by 1,2
order by 3 desc;

-- states with most sales and what profit they generate
select state, count(*) as total_sales, sum(profit) as total_profit
from store_copy
group by 1
order by 2 desc, 3 desc;

-- does less profit occur due to high discounts?
select profit, discount
from store_copy
order by 1;  -- almost every sale at loss has provided high discount so YES

-- sub-quantities with the most units sold
select sub_category, sum(quantity) as total_quantity
from store_copy
group by 1
order by 2 desc;

-- average shipping time
select avg(datediff(ship_date, order_date)) as avg_shipping_time
from store_copy; -- an average of 4days

-- average shipping time by shipping mode
select ship_mode, avg(datediff(ship_date, order_date)) as avg_shipping_time
from store_copy
group by 1
order by 2 desc;


-- profit margin of each category
select sub_category, (SUM(profit)/SUM(sales))*100 as margin_percent
from store_copy
group by 1
order by 2 desc;

-- rank years by total sales
select year(order_date) as year,
sum(sales) as total_sales, sum(profit) as total_profit
from store_copy
group by year
order by total_sales;

-- months with the most sales 
select monthname(order_date) as mnth,
sum(sales) as total_sales, sum(profit) as total_profit
from store_copy
group by mnth
order by 2 desc, 3 desc;

-- which category should lower their discounts
select category,
avg(discount) avg_discount,
avg(profit) avg_profit
from store_copy
group by 1
order by 3 asc;


-- does items with high discount have higher no of orders
select sub_category,
avg(discount) as avg_discount,
count(distinct order_id) as total_distinct_orders
from store_copy
group by 1
order by 2 desc;  -- not necessarily
