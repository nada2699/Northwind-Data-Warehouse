------------------------------------------------------------------------------------------------------------------
--------------- Update Customers Table ------------------------------
------------------------------------------------------------------------------------------------------------------
Alter Table customers
Add column last_update date;

Select * FROM customers;
------------------------------------------------------------------------------------------------------------------
--------------- Update Order details Table ------------------------------
------------------------------------------------------------------------------------------------------------------
Alter Table order_details 
Add column order_date date;

UPDATE order_details od
SET order_date = o.order_date
FROM orders o
WHERE od.order_id= o.order_id;

SELECT * FROM order_details;
------------------------------------------------------------------------------------------------------------------
--------------- Update Products Table ------------------------------
------------------------------------------------------------------------------------------------------------------
Alter Table products 
Add column last_update date;

SELECT * FROM products;
------------------------------------------------------------------------------------------------------------------
--------------- Update Employees Table ------------------------------
------------------------------------------------------------------------------------------------------------------
Alter Table employees 
Add column last_update date;

SELECT * FROM employees;
------------------------------------------------------------------------------------------------------------------
--------------- Update Shippers Table ------------------------------
------------------------------------------------------------------------------------------------------------------
Alter Table shippers  
Add column last_update date;

SELECT * FROM shippers;
------------------------------------------------------------------------------------------------------------------
--------------- Update Suppliers Table ------------------------------
------------------------------------------------------------------------------------------------------------------
Alter Table suppliers  
Add column last_update date;

SELECT * FROM suppliers;
------------------------------------------------------------------------------------------------------------------
--------------- Orders Table ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.bro_orders;
CREATE TABLE bronze.bro_orders (
	order_id int2 NOT NULL,
	customer_id varchar(5) NULL,
	employee_id int2 NULL,
	order_date date NULL,
	required_date date NULL,
	shipped_date date NULL,
	ship_via int2 NULL,
	freight float4 NULL,
	ship_name varchar(40) NULL,
	ship_address varchar(60) NULL,
	ship_city varchar(15) NULL,
	ship_region varchar(15) NULL,
	ship_postal_code varchar(10) NULL,
	ship_country varchar(15) NULL
);

-- Intital Load
INSERT INTO bronze.bro_orders
SELECT * FROM orders;

--Incremental Load
TRUNCATE TABLE bronze.bro_orders;
INSERT INTO bronze.bro_orders 
SELECT * FROM orders
WHERE order_date = current_date - 1;

SELECT * FROM bronze.bro_orders;
------------------------------------------------------------------------------------------------------------------
--------------- Order details Table ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.bro_order_details;
CREATE TABLE bronze.bro_order_details (
	order_id int2 NOT NULL,
	product_id int2 NOT NULL,
	unit_price float4 NOT NULL,
	quantity int2 NOT NULL,
	discount float4 NOT NULL,
	order_date date NULL
);

-- Intital Load
INSERT INTO bronze.bro_order_details
SELECT * FROM order_details;

--Incremental Load
TRUNCATE TABLE bronze.bro_order_details;
INSERT INTO bronze.bro_order_details 
SELECT * FROM order_details
WHERE order_date = current_date - 1;

SELECT * FROM bronze.bro_order_details;
------------------------------------------------------------------------------------------------------------------
--------------- Products Table ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.bro_products;
CREATE TABLE bronze.bro_products (
	product_id int2 NOT NULL,
	product_name varchar(40) NOT NULL,
	supplier_id int2 NULL,
	category_id int2 NULL,
	quantity_per_unit varchar(20) NULL,
	unit_price float4 NULL,
	units_in_stock int2 NULL,
	units_on_order int2 NULL,
	reorder_level int2 NULL,
	discontinued int4 NOT NULL,
	last_update date NULL
);

-- Intital Load
INSERT INTO bronze.bro_products
SELECT * FROM products;

--Incremental Load
TRUNCATE TABLE bronze.bro_products;
INSERT INTO bronze.bro_products
SELECT * FROM products
WHERE last_update = current_date - 1;

SELECT * FROM  bronze.bro_products;
------------------------------------------------------------------------------------------------------------------
--------------- Customers Table ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.bro_customers;
CREATE TABLE bronze.bro_customers (
	customer_id varchar(5) NOT NULL,
	company_name varchar(40) NOT NULL,
	contact_name varchar(30) NULL,
	contact_title varchar(30) NULL,
	address varchar(60) NULL,
	city varchar(15) NULL,
	region varchar(15) NULL,
	postal_code varchar(10) NULL,
	country varchar(15) NULL,
	phone varchar(24) NULL,
	fax varchar(24) NULL,
	last_update date NULL
);

--Intial Load
INSERT INTO bronze.bro_customers
SELECT * FROM customers;

--Incremental Load
TRUNCATE TABLE bronze.bro_customers;
INSERT INTO bronze.bro_customers
SELECT * FROM customers
WHERE last_update = current_date - 1;

SELECT * FROM bronze.bro_customers;
------------------------------------------------------------------------------------------------------------------
--------------- Categories Table ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.bro_categories;
CREATE TABLE bronze.bro_categories (
	category_id int2 NOT NULL,
	category_name varchar(15) NOT NULL,
	description text NULL,
	picture bytea NULL
);

--Full Load
TRUNCATE TABLE bronze.bro_categories;
INSERT INTO bronze.bro_categories
SELECT * FROM categories;

SELECT * FROM bronze.bro_categories;
------------------------------------------------------------------------------------------------------------------
--------------- Employees Table ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.bro_employees;
CREATE TABLE bronze.bro_employees (
	employee_id int2 NOT NULL,
	last_name varchar(20) NOT NULL,
	first_name varchar(10) NOT NULL,
	title varchar(30) NULL,
	title_of_courtesy varchar(25) NULL,
	birth_date date NULL,
	hire_date date NULL,
	address varchar(60) NULL,
	city varchar(15) NULL,
	region varchar(15) NULL,
	postal_code varchar(10) NULL,
	country varchar(15) NULL,
	home_phone varchar(24) NULL,
	"extension" varchar(4) NULL,
	photo bytea NULL,
	notes text NULL,
	reports_to int2 NULL,
	photo_path varchar(255) NULL,
	last_update date NULL
);

--Intial Load
INSERT INTO bronze.bro_employees
SELECT * FROM employees;

--Incremental Load
TRUNCATE TABLE bronze.bro_employees;
INSERT INTO bronze.bro_employees
SELECT * FROM employees
WHERE last_update = current_date - 1;
------------------------------------------------------------------------------------------------------------------
--------------- Shippers Table ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.bro_shippers;
CREATE TABLE bronze.bro_shippers (
	shipper_id int2 NOT NULL,
	company_name varchar(40) NOT NULL,
	phone varchar(24) NULL,
	last_update date NULL
);

--Intial Load
INSERT INTO bronze.bro_shippers
SELECT * FROM shippers;

--Incremental Load
TRUNCATE TABLE bronze.bro_shippers;
INSERT INTO bronze.bro_shippers
SELECT * FROM shippers
WHERE last_update = current_date - 1;

SELECT * FROM bronze.bro_shippers;
------------------------------------------------------------------------------------------------------------------
--------------- Suppliers Table ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.bro_suppliers;
CREATE TABLE bronze.bro_suppliers (
	supplier_id int2 NOT NULL,
	company_name varchar(40) NOT NULL,
	contact_name varchar(30) NULL,
	contact_title varchar(30) NULL,
	address varchar(60) NULL,
	city varchar(15) NULL,
	region varchar(15) NULL,
	postal_code varchar(10) NULL,
	country varchar(15) NULL,
	phone varchar(24) NULL,
	fax varchar(24) NULL,
	homepage text NULL,
	last_update date NULL
);

--Intial Load
INSERT INTO bronze.bro_suppliers
SELECT * FROM suppliers;

--Incremental Load
TRUNCATE TABLE bronze.bro_suppliers;
INSERT INTO bronze.bro_suppliers
SELECT * FROM suppliers
WHERE last_update = current_date - 1;

SELECT * FROM bronze.bro_suppliers;
-------------------------------------------------------------------------------------------------------------------

SELECT order_date FROM orders
WHERE order_date>current_date;



