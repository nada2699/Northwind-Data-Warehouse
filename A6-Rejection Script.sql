DROP TABLE IF EXISTS rejtables.orders;
CREATE TABLE rejtables.orders (
    order_id INT,
    customer_id varchar(5) NULL,
	employee_id int2 NULL,
    order_date DATE,
    shipped_date DATE,
    rejection_reason VARCHAR(255),
    rejection_timestamp TIMESTAMP
);

TRUNCATE rejtables.orders;
SELECT count(*) FROM rejtables.orders where rejection_reason='Duplicated Order';
SELECT * FROM rejtables.orders where rejection_reason='Duplicated Order' and order_id=10255;
SELECT *  FROM rejtables.orders where rejection_reason='Uncompleted Order';
SELECT *  FROM rejtables.orders where rejection_reason='Invalid future date';

SELECT count(*) FROM rejtables.orders;
SELECT * FROM rejtables.orders;
----------------------------------------------------------------------------------------
DROP TABLE IF EXISTS rejtables.order_details;
CREATE TABLE rejtables.order_details (
	order_id int2 NOT NULL,
	product_id int2 NOT NULL,
	unit_price float4 NOT NULL,
	quantity int2 NOT NULL,
	discount float4 NOT NULL,
	order_date date NULL,
    rejection_reason VARCHAR(255),
    rejection_timestamp TIMESTAMP
);

TRUNCATE rejtables.order_details;
SELECT count(*) FROM rejtables.order_details;
SELECT * FROM rejtables.order_details;
----------------------------------------------------------------------------------------
DROP TABLE IF EXISTS rejtables.customers;
CREATE TABLE rejtables.customers (
	customer_id varchar(5) NULL,
	company_name varchar(40) NULL,
	contact_name varchar(30) NULL,
	phone varchar(24) NULL,
	country varchar(15) NULL,
    rejection_reason VARCHAR(255),
    rejection_timestamp TIMESTAMP
);

SELECT count(*) FROM bronze.bro_customers;
TRUNCATE rejtables.customers;
SELECT * FROM rejtables.customers;
SELECT count(*) FROM rejtables.customers;
----------------------------------------------------------------------------------------
DROP TABLE IF EXISTS rejtables.employees;
CREATE TABLE rejtables.employees (
	employee_id int2 NOT NULL,
	last_name varchar(20) NOT NULL,
	first_name varchar(10) NOT NULL,
	title varchar(30) NULL,
	hire_date date NULL,
	country varchar(15) NULL,
	home_phone varchar(24) NULL,
    rejection_reason VARCHAR(255),
    rejection_timestamp TIMESTAMP
);
TRUNCATE rejtables.employees;
SELECT * FROM rejtables.employees;
SELECT count(*) FROM rejtables.employees;
----------------------------------------------------------------------------------------
DROP TABLE IF EXISTS rejtables.categories;
CREATE TABLE rejtables.categories (
	category_id int2 NOT NULL,
	category_name varchar(15) NOT NULL,
	description text NULL,
    rejection_reason VARCHAR(255),
    rejection_timestamp TIMESTAMP
);
TRUNCATE rejtables.categories;
SELECT * FROM rejtables.categories;
SELECT count(*) FROM rejtables.categories;
----------------------------------------------------------------------------------------
DROP TABLE IF EXISTS rejtables.products;
CREATE TABLE rejtables.products (
	product_id int2 NOT NULL,
	product_name varchar(40) NOT NULL,
	supplier_id int2 NULL,
	category_id int2 NULL,
    rejection_reason VARCHAR(255),
    rejection_timestamp TIMESTAMP
);
TRUNCATE rejtables.products;
SELECT * FROM rejtables.products;
SELECT count(*) FROM rejtables.products;
----------------------------------------------------------------------------------------
DROP TABLE IF EXISTS rejtables.shippers;
CREATE TABLE rejtables.shippers (
	shipper_id int2 NOT NULL,
	company_name varchar(40) NOT NULL,
	phone varchar(24) NULL,
    rejection_reason VARCHAR(255),
    rejection_timestamp TIMESTAMP
);
TRUNCATE rejtables.shippers;
SELECT * FROM rejtables.shippers;
SELECT count(*) FROM rejtables.shippers;
----------------------------------------------------------------------------------------
DROP TABLE IF EXISTS rejtables.suppliers;
CREATE TABLE rejtables.suppliers (
	supplier_id int2 NOT NULL,
	company_name varchar(40) NOT NULL,
	contact_name varchar(30) NULL,
	contact_title varchar(30) NULL,
	postal_code varchar(10) NULL,
	phone varchar(24) NULL,
    rejection_reason VARCHAR(255),
    rejection_timestamp TIMESTAMP
);
TRUNCATE rejtables.suppliers;
SELECT * FROM rejtables.suppliers;
SELECT count(*) FROM rejtables.suppliers;

















