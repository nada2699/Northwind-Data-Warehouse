------------------------------------------------------------------------------------------------------------------
--------------- Orders Table ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.sil_orders;
CREATE TABLE silver.sil_orders (
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
	ship_country varchar(15) NULL,
	valid_from date NULL,
	valid_to date,
	valid_flag BOOLEAN
);

--Insert Duplicated records into rejection table
WITH duplicates AS (
SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_id) AS row_num
FROM bronze.bro_orders
)
INSERT INTO rejtables.orders
SELECT order_id, 
	   customer_id, 
	   employee_id, 
	   order_date,
	   shipped_date,
	   'Duplicated Order' AS rejection_reason,
	   CURRENT_TIMESTAMP AS rejection_timestamp
FROM duplicates
WHERE row_num > 1;


-- Insert Distinct Records in temporary table
DROP TABLE IF EXISTS ins_dist_orders;
CREATE TEMPORARY TABLE ins_dist_orders AS
SELECT DISTINCT * FROM bronze.bro_orders;

SELECT * FROM ins_dist_orders;
-- Insert Invalid order dates and shipped dates into rejection table
INSERT INTO rejtables.orders
SELECT order_id,customer_id,employee_id,order_date,shipped_date,
      CASE WHEN order_date > CURRENT_DATE THEN 'Invalid future date' 
      WHEN shipped_date IS NULL THEN 'Uncompleted Order'END AS rejection_reason,
      CURRENT_TIMESTAMP AS rejection_timestamp
FROM ins_dist_orders
WHERE shipped_date IS NULL OR order_date > CURRENT_DATE;

-- Insert and update into silver stage's orders table distinct, valid and transformed values
TRUNCATE silver.sil_orders;
WITH transformed_orders AS (
    SELECT order_id, customer_id, employee_id, order_date, required_date, COALESCE(shipped_date,'2099-12-31') as shipped_date,
    ship_via,freight, ship_name, ship_address, ship_city,
    COALESCE(ship_region,'NA') as ship_region, ship_postal_code, ship_country
FROM ins_dist_orders
WHERE order_date <= CURRENT_DATE
)
MERGE INTO silver.sil_orders tgt
USING transformed_orders src
ON src.order_id = tgt.order_id
WHEN MATCHED THEN
UPDATE
SET valid_to = src.order_date-1, valid_flag = FALSE
WHEN NOT MATCHED THEN
INSERT(order_id, customer_id, employee_id, order_date, required_date, shipped_date, ship_via, freight, ship_name, ship_address, ship_city, ship_region, ship_postal_code, ship_country, valid_from, valid_to, valid_flag)
VALUES (src.order_id, src.customer_id, src.employee_id, src.order_date, src.required_date, src.shipped_date, src.ship_via, src.freight, src.ship_name, src.ship_address, src.ship_city, src.ship_region, src.ship_postal_code, src.ship_country, src.order_date, '2099-12-31', TRUE);

INSERT INTO silver.sil_orders (order_id, customer_id, employee_id, order_date, required_date, shipped_date, ship_via, freight, ship_name, ship_address, ship_city, ship_region, ship_postal_code, ship_country,valid_from,valid_to,valid_flag)
SELECT src.order_id, src.customer_id, src.employee_id, src.order_date, src.required_date, COALESCE(src.shipped_date,'2099-12-31'), src.ship_via, src.freight, src.ship_name, src.ship_address, src.ship_city,COALESCE(src.ship_region,'NA'), src.ship_postal_code, src.ship_country, src.order_date, '2099-12-31', TRUE
FROM ins_dist_orders src
LEFT JOIN silver.sil_orders tgt 
ON src.order_id = tgt.order_id
AND tgt.valid_flag = TRUE
AND src.order_date <= CURRENT_DATE ;

---------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------
--------------- Order Details Table ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.sil_order_details;
CREATE TABLE silver.sil_order_details (
	order_id int2 NOT NULL,
	product_id int2 NOT NULL,
	unit_price float4 NOT NULL,
	quantity int2 NOT NULL,
	discount float4 NOT NULL,
	order_date date NULL,
	valid_from date NULL,
	valid_to date NULL,
	valid_flag bool NULL
);

--Insert Duplicated records into rejection table
WITH duplicates AS (
SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id,product_id ORDER BY order_id,product_id) AS row_num
FROM bronze.bro_order_details 
)
INSERT INTO rejtables.order_details
SELECT order_id,
	   product_id,
	   unit_price,
	   quantity,
	   discount,
	   order_date,
	   'Duplicated Order' AS rejection_reason,
	   CURRENT_TIMESTAMP AS rejection_timestamp
FROM duplicates
WHERE row_num > 1;

-- Insert Distinct Records in temporary table
DROP TABLE IF EXISTS ins_dist_order_details;
CREATE TEMPORARY TABLE ins_dist_order_details AS
SELECT DISTINCT * FROM bronze.bro_order_details;

SELECT count(*) FROM ins_dist_order_details;

TRUNCATE silver.sil_order_details;

MERGE INTO silver.sil_order_details tgt
USING ins_dist_order_details src
ON src.order_id = tgt.order_id
AND src.product_id = tgt.product_id
WHEN MATCHED THEN
UPDATE
SET valid_to = src.order_date-1, valid_flag = FALSE
WHEN NOT MATCHED THEN
INSERT(order_id, product_id, unit_price, quantity, discount, order_date, valid_from, valid_to, valid_flag)
VALUES (src.order_id, src.product_id, src.unit_price, src.quantity, COALESCE(src.discount, 0), src.order_date, src.order_date, '2099-12-31', TRUE);

INSERT INTO silver.sil_order_details (order_id, product_id, unit_price, quantity, discount, order_date, valid_from, valid_to, valid_flag)
SELECT src.order_id, src.product_id, src.unit_price, src.quantity, COALESCE(src.discount, 0), src.order_date, src.order_date, '2099-12-31', TRUE
FROM ins_dist_order_details src
INNER JOIN silver.sil_order_details tgt 
ON src.order_id = tgt.order_id
AND tgt.valid_flag = FALSE;

SELECT * FROM silver.sil_order_details;
---------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------
--------------- Customers Table ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.sil_customers;
CREATE TABLE silver.sil_customers (
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
	valid_from date NULL,
	valid_to date NULL,
	valid_flag boolean
);

--Insert Duplicated records into rejection table
WITH duplicates AS (
SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) AS row_num
FROM bronze.bro_customers
)
INSERT INTO rejtables.customers
SELECT customer_id,
	   company_name,
	   contact_name,
	   phone,
	   country,
       'Duplicated Customer' AS rejection_reason,
  	   CURRENT_TIMESTAMP AS rejection_timestamp
FROM duplicates
WHERE row_num > 1;

-- Insert Distinct Records into a temporary table
DROP TABLE IF EXISTS ins_dist_customers;
CREATE TEMPORARY TABLE ins_dist_customers AS
SELECT DISTINCT * FROM bronze.bro_customers;

SELECT * FROM ins_dist_customers;

-- Insert and update into silver stage's customers table distinct, valid and transformed values
TRUNCATE silver.sil_customers;
WITH transformed_customers AS (
    SELECT customer_id, company_name, contact_name, contact_title, address, city,
    CASE WHEN region IS NULL OR region = 'IncorrectRegion' THEN 'NA'
	ELSE region END AS region, COALESCE(postal_code, 'NA') AS postal_code, country, COALESCE(REGEXP_REPLACE(phone, '[. ]', '-', 'g'),'NA') AS phone, COALESCE(REGEXP_REPLACE(fax, '[. ]', '-', 'g'), 'NA') AS fax, COALESCE(last_update,'1937-01-01') AS last_update
FROM ins_dist_customers
)
MERGE INTO silver.sil_customers tgt
USING transformed_customers src
ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN
UPDATE
SET valid_to = src.last_update - 1, valid_flag = FALSE
WHEN NOT MATCHED THEN
INSERT(customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, valid_from, valid_to, valid_flag)
VALUES (src.customer_id, src.company_name, src.contact_name, src.contact_title, src.address, src.city, src.region, src.postal_code, src.country, src.phone, src.fax, src.last_update, '2099-12-31', TRUE);


INSERT INTO silver.sil_customers 
(customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, valid_from, valid_to, valid_flag)
SELECT src.customer_id, src.company_name, src.contact_name, src.contact_title, src.address, src.city, COALESCE(src.region, 'NA'), COALESCE(src.postal_code, 'NA'), src.country, COALESCE(REGEXP_REPLACE(src.phone, '[. ]', '-', 'g'),'NA'), COALESCE(regexp_replace(src.fax, '[. ]', '-', 'g'), 'NA'),src.last_update, '2099-12-31', TRUE
FROM ins_dist_customers src
INNER JOIN silver.sil_customers tgt 
ON
src.customer_id = tgt.customer_id
AND tgt.valid_flag = FALSE;

SELECT * FROM silver.sil_customers;
---------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------
--------------- Employees Table ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.sil_employees;
CREATE TABLE silver.sil_employees (
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
	valid_from date NULL,
	valid_to date NULL,
	valid_flag boolean
);

--Insert Duplicated records into rejection table
WITH duplicates AS (
SELECT *, ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY employee_id) AS row_num
FROM bronze.bro_employees
)
INSERT INTO rejtables.employees
SELECT employee_id,
		last_name,
		first_name,
		title,
		hire_date,
		country,
		home_phone,
	    'Duplicated Employee' AS rejection_reason,
	    CURRENT_TIMESTAMP AS rejection_timestamp
FROM duplicates
WHERE row_num > 1;

-- Insert Distinct Records into a temporary table
DROP TABLE IF EXISTS ins_dist_employees;
CREATE TEMPORARY TABLE ins_dist_employees AS
SELECT DISTINCT * FROM bronze.bro_employees;

SELECT * FROM ins_dist_employees;

-- Insert and update into silver stage's employees table distinct, valid and transformed values
TRUNCATE silver.sil_employees;
MERGE INTO silver.sil_employees tgt
USING ins_dist_employees src
ON tgt.employee_id = src.employee_id
WHEN MATCHED THEN
UPDATE
SET valid_to = src.last_update - 1, valid_flag = FALSE
WHEN NOT MATCHED THEN
INSERT(employee_id, last_name, first_name, title, title_of_courtesy, birth_date, hire_date, address, city, region, postal_code, country, home_phone, "extension", photo, notes, reports_to, photo_path,valid_from,valid_to,valid_flag)
VALUES(src.employee_id, src.last_name, src.first_name, src.title, src.title_of_courtesy, src.birth_date, src.hire_date, src.address, src.city, COALESCE(src.region,'NA'), src.postal_code, src.country, src.home_phone, src."extension", src.photo, src.notes, src.reports_to, src.photo_path,COALESCE(src.last_update,src.hire_date),'2099-12-31',TRUE);

INSERT INTO silver.sil_employees (employee_id, last_name, first_name, title, title_of_courtesy, birth_date, hire_date, address, city, region, postal_code, country, home_phone, "extension", photo, notes, reports_to, photo_path,valid_from,valid_to,valid_flag)
SELECT src.employee_id, src.last_name, src.first_name, src.title, src.title_of_courtesy, src.birth_date, src.hire_date, src.address, src.city, COALESCE(src.region,'NA'), src.postal_code, src.country, src.home_phone, src."extension", src.photo, src.notes, src.reports_to, src.photo_path, src.last_update, '2099-12-31', TRUE
FROM ins_dist_employees src
INNER JOIN silver.sil_employees tgt 
ON
src.employee_id = tgt.employee_id
AND tgt.valid_flag = FALSE;

SELECT * FROM silver.sil_employees;
---------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------
--------------- Categories Table ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.sil_categories;
CREATE TABLE silver.sil_categories (
	category_id int2 NOT NULL,
	category_name varchar(15) NOT NULL,
	description text NULL,
	picture bytea NULL
);

--Insert Duplicated records into rejection table
WITH duplicates AS (
SELECT *, ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY category_id) AS row_num
FROM bronze.bro_categories 
)
INSERT INTO rejtables.categories
SELECT category_id, 
	   category_name, 
	   description,
	   'Duplicated Category' AS rejection_reason,
	   CURRENT_TIMESTAMP AS rejection_timestamp
FROM duplicates
WHERE row_num > 1;

-- Insert and update into silver stage's categories table distinct, valid and transformed values
TRUNCATE silver.sil_categories;
-- Insert Distinct Records in a CTE
WITH distincts AS ( 
SELECT DISTINCT *
FROM bronze.bro_categories 
)
MERGE INTO silver.sil_categories tgt
USING distincts src
ON tgt.category_id = src.category_id
WHEN MATCHED THEN
UPDATE
SET  category_id=src.category_id, 
	category_name=src.category_name, 
	description=src.description,
	picture=src.picture
WHEN NOT MATCHED THEN
INSERT(category_id, category_name, description, picture)
VALUES(src.category_id, src.category_name, src.description, src.picture);



SELECT * FROM silver.sil_categories;
---------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------
--------------- Products Table ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.sil_products;
CREATE TABLE silver.sil_products (
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
	valid_from date NULL,
	valid_to date NULL,
	valid_flag bool NULL
);

--Insert Duplicated records into rejection table
WITH duplicates AS (
SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS row_num
FROM bronze.bro_products
)
INSERT INTO rejtables.products
SELECT product_id,
	   product_name,
	   supplier_id,
	   category_id,
   	   'Duplicated Product' AS rejection_reason,
    	CURRENT_TIMESTAMP AS rejection_timestamp
FROM duplicates
WHERE row_num > 1;

-- Insert Distinct Records in a temporary table
DROP TABLE IF EXISTS ins_dist_products;
CREATE TEMPORARY TABLE ins_dist_products AS
SELECT DISTINCT * FROM bronze.bro_products;

SELECT * FROM ins_dist_products;

-- Insert and update into silver stage's categories table distinct, valid and transformed values
TRUNCATE silver.sil_products;
MERGE INTO silver.sil_products tgt
USING ins_dist_products src
ON tgt.product_id = src.product_id
WHEN MATCHED THEN
UPDATE
SET valid_to = src.last_update-1, valid_flag = FALSE
WHEN NOT MATCHED THEN
INSERT (product_id, product_name, supplier_id, category_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued, valid_from, valid_to, valid_flag)
VALUES(src.product_id, src.product_name, src.supplier_id, src.category_id, src.quantity_per_unit, src.unit_price, src.units_in_stock, src.units_on_order, src.reorder_level, src.discontinued, COALESCE(src.last_update,'1937-01-01'),'2099-12-31',TRUE);

INSERT INTO silver.sil_products 
(product_id, product_name, supplier_id, category_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued, valid_from, valid_to, valid_flag)
SELECT src.product_id, src.product_name, src.supplier_id, src.category_id, src.quantity_per_unit, src.unit_price, src.units_in_stock, src.units_on_order, src.reorder_level, src.discontinued, src.last_update,'2099-12-31',TRUE
FROM ins_dist_products src
JOIN silver.sil_products tgt
ON src.product_id=tgt.product_id
AND valid_flag=FALSE;


SELECT * FROM silver.sil_products;
---------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------
--------------- Shippers Table ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.sil_shippers;
CREATE TABLE silver.sil_shippers (
	shipper_id int2 NOT NULL,
	company_name varchar(40) NOT NULL,
	phone varchar(24) NULL,
	valid_from date NULL,
	valid_to date NULL,
	valid_flag bool NULL
);

--Insert Duplicated records into rejection table
WITH duplicates AS (
SELECT *, ROW_NUMBER() OVER (PARTITION BY shipper_id ORDER BY shipper_id) AS row_num
FROM bronze.bro_shippers
)
INSERT INTO rejtables.shippers
SELECT shipper_id,
	   company_name,
	   phone,
   	   'Duplicated Shipper' AS rejection_reason,
    	CURRENT_TIMESTAMP AS rejection_timestamp
FROM duplicates
WHERE row_num > 1;

-- Insert Distinct Records in a temporary table
DROP TABLE IF EXISTS ins_dist_shippers;
CREATE TEMPORARY TABLE ins_dist_shippers AS
SELECT DISTINCT * FROM bronze.bro_shippers;

SELECT * FROM ins_dist_shippers;

TRUNCATE silver.sil_shippers;
MERGE INTO silver.sil_shippers tgt
USING ins_dist_shippers src
ON tgt.shipper_id = src.shipper_id
WHEN MATCHED THEN
UPDATE
SET valid_to = src.last_update - 1, valid_flag = FALSE
WHEN NOT MATCHED THEN
INSERT (shipper_id, company_name, phone, valid_from, valid_to, valid_flag)
VALUES (src.shipper_id, src.company_name, src.phone,  COALESCE(src.last_update,'1937-01-01'),'2099-12-31',TRUE);

INSERT INTO silver.sil_shippers (shipper_id, company_name, phone, valid_from, valid_to, valid_flag)
SELECT src.shipper_id, src.company_name, src.phone, src.last_update,'2099-12-31',TRUE
FROM ins_dist_shippers src
JOIN silver.sil_shippers tgt
ON src.shipper_id=tgt.shipper_id
AND valid_flag=FALSE;

SELECT * FROM silver.sil_shippers;
-----------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------
--------------- Suppliers Table ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.sil_suppliers;
CREATE TABLE silver.sil_suppliers (
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
	valid_from date NULL,
	valid_to date NULL,
	valid_flag bool NULL
);

--Insert Duplicated records into rejection table
WITH duplicates AS (
SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY supplier_id) AS row_num
FROM bronze.bro_suppliers
)
INSERT INTO rejtables.suppliers
SELECT supplier_id,
	   company_name,
	   contact_name,
	   contact_title,
	   postal_code,
	   phone, 
   	   'Duplicated Supplier' AS rejection_reason,
       CURRENT_TIMESTAMP AS rejection_timestamp
FROM duplicates
WHERE row_num > 1;

-- Insert Distinct Records in a temporary table
DROP TABLE IF EXISTS ins_dist_suppliers;
CREATE TEMPORARY TABLE ins_dist_suppliers AS
SELECT DISTINCT * FROM bronze.bro_suppliers;

SELECT * FROM ins_dist_suppliers;

TRUNCATE silver.sil_suppliers;
MERGE INTO silver.sil_suppliers tgt
USING ins_dist_suppliers src
ON tgt.supplier_id = src.supplier_id
WHEN MATCHED THEN
UPDATE
SET valid_to = src.last_update - 1, valid_flag = FALSE
WHEN NOT MATCHED THEN
INSERT (supplier_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, homepage, valid_from, valid_to, valid_flag)
VALUES (src.supplier_id, src.company_name, src.contact_name, src.contact_title, src.address, src.city,COALESCE(src.region,'NA'), src.postal_code, src.country, COALESCE(REGEXP_REPLACE(src.phone, '[. ]', '-', 'g'),'NA'), COALESCE(regexp_replace(src.fax, '[. ]', '-', 'g'), 'NA'), COALESCE(src.homepage,'NA'), COALESCE(src.last_update,'1937-01-01'),'2099-12-31',TRUE);

INSERT INTO silver.sil_suppliers (supplier_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, homepage, valid_from, valid_to, valid_flag)
SELECT src.supplier_id, src.company_name, src.contact_name, src.contact_title, src.address, src.city, COALESCE(src.region,'NA'), src.postal_code, src.country,  COALESCE(REGEXP_REPLACE(src.phone, '[. ]', '-', 'g'),'NA'), COALESCE(regexp_replace(src.fax, '[. ]', '-', 'g'), 'NA'), COALESCE(src.homepage,'NA'), src.last_update,'2099-12-31',TRUE
FROM ins_dist_suppliers src
JOIN silver.sil_suppliers tgt
ON src.supplier_id=tgt.supplier_id
AND valid_flag=FALSE;

SELECT * FROM silver.sil_suppliers;

