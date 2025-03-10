------------------------------------------------------------------------------------------------------------------
--------------- Country Dimension ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.country_dim;
CREATE TABLE gold.country_dim (
	country_dim_key serial PRIMARY KEY NOT NULL,
	country_name varchar(40) NOT NULL
);

-- source tables: --> customers  orders suppliers employees
DROP TABLE IF EXISTS src_country;
CREATE TEMPORARY TABLE src_country AS
SELECT DISTINCT country as country_name
FROM silver.sil_customers
WHERE valid_flag=TRUE
UNION
SELECT DISTINCT country as country_name
FROM silver.sil_employees
WHERE valid_flag=TRUE
UNION
SELECT DISTINCT country as country_name
FROM silver.sil_suppliers
WHERE valid_flag=TRUE
UNION
SELECT DISTINCT ship_country as country_name
FROM silver.sil_orders
WHERE valid_flag=TRUE;

SELECT * FROM src_country;

TRUNCATE TABLE gold.country_dim;

MERGE INTO gold.country_dim tgt
USING src_country src
ON upper(src.country_name) = upper(tgt.country_name)
WHEN NOT MATCHED THEN
  INSERT(country_name) 
  VALUES (src.country_name);


SELECT * FROM gold.country_dim;
------------------------------------------------------------------------------------------------------------------
--------------- Customers Dimension ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS  gold.customers_dim;
CREATE TABLE gold.customers_dim (
	dwh_cust_key serial PRIMARY KEY NOT NULL,
	customer_id varchar(5) NOT NULL,
	customer_name varchar(40) NOT NULL,
	customer_title varchar(40) NULL,
	customer_company varchar(40) NULL,
	address varchar(60) NULL,
	city varchar(15) NULL,
	country_id int NULL,
	region varchar(15) NULL,	
	phone varchar(24) NULL,
	fax varchar(24) NULL,
	postal_code varchar(10) NULL,
	valid_from date NULL,
	valid_to date NULL,
	valid_flag bool NULL,
	FOREIGN KEY (country_id) REFERENCES gold.country_dim (country_dim_key)
);


TRUNCATE gold.customers_dim;
MERGE INTO gold.customers_dim tgt
USING 
(SELECT customer_id, company_name AS customer_company, contact_name AS customer_name , contact_title AS customer_title, address, city, region, postal_code, phone, fax, valid_from, valid_to, valid_flag,cd.country_dim_key AS country_id
FROM silver.sil_customers sc 
JOIN gold.country_dim cd 
ON sc.country=cd.country_name) as src
ON tgt.customer_id = src.customer_id
WHEN MATCHED AND (src.customer_name <> tgt.customer_name OR
				  src.customer_title <> tgt.customer_title OR
				  src.customer_company <> tgt.customer_company OR
				  src.address <> tgt.address OR
				  src.city <> tgt.city OR
				  src.country_id <> tgt.country_id OR
				  src.region <> tgt.region OR
				  src.phone <> tgt.phone OR
				  src.fax <> tgt.fax OR
				  src.postal_code <> tgt.postal_code 
				  ) THEN 
UPDATE SET valid_to = CURRENT_DATE - INTERVAL '1 DAY', valid_flag = FALSE
WHEN NOT MATCHED THEN
INSERT (customer_id, customer_name, customer_title, customer_company, address, city, country_id, region, phone, fax, postal_code, valid_from, valid_to, valid_flag)
VALUES (src.customer_id, src.customer_name, src.customer_title, src.customer_company, src.address, src.city, src.country_id, src.region, src.phone, src.fax, src.postal_code, src.valid_from, src.valid_to,TRUE);

INSERT INTO gold.customers_dim (customer_id, customer_name, customer_title, customer_company, address, city, country_id, region, phone, fax, postal_code, valid_from, valid_to, valid_flag)
SELECT src.customer_id, src.contact_name AS customer_name, src.contact_title AS customer_title, src.company_name AS customer_company ,src.address, src.city, cd.country_dim_key AS country_id,src.region,src.phone, src.fax,src.postal_code, CURRENT_DATE,'2099-12-31',TRUE
FROM silver.sil_customers src
JOIN gold.country_dim cd 
ON src.country=cd.country_name
JOIN gold.customers_dim tgt
ON src.customer_id=tgt.customer_id
AND tgt.valid_flag=FALSE
AND src.valid_flag =TRUE;



SELECT * FROM gold.customers_dim;
------------------------------------------------------------------------------------------------------------------
--------------- Date Dimension ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.date_dim;
CREATE TABLE gold.date_dim (
	dwh_date_key serial PRIMARY KEY NOT NULL,
	full_date date NOT NULL,
	week_day varchar(10) NOT NULL,
	week_of_month int NOT NULL,
	month varchar(10) NOT NULL,
	quarter varchar(10) NOT NULL,
	year int NOT NULL
);
TRUNCATE gold.date_dim ;
INSERT INTO gold.date_dim (
    full_date, week_day, week_of_month, month, quarter, year
)
SELECT 
     gs::DATE AS full_date,
    to_char(gs, 'Day') AS week_day,
    to_char(gs, 'W') AS week_of_month,
    to_char(gs, 'Month') AS month,
    to_char(gs, 'Q') AS quarter,
    extract(year FROM gs) AS year
FROM generate_series('1937-01-01'::DATE, '2100-01-01'::DATE, '1 day'::INTERVAL) AS gs;

SELECT * FROM gold.date_dim;

------------------------------------------------------------------------------------------------------------------
--------------- Date Dimension ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.employees_dim;
CREATE TABLE gold.employees_dim (
	dwh_emp_key serial PRIMARY KEY NOT NULL,
	employee_id int2 NOT NULL,
	emp_name varchar(50) NOT NULL,
	title varchar(30) NULL,
	birth_date int NULL,
	hire_date int NULL,
	address varchar(60) NULL,
	city varchar(15) NULL,
	country_id int NULL,
	region varchar(15) NULL,
	postal_code varchar(10) NULL,
	phone varchar(24) NULL,
	"extension" varchar(4) NULL,
	manager int2 NULL,
	valid_from date NULL,
	valid_to date NULL,
	valid_flag bool NULL,
	FOREIGN KEY (birth_date) REFERENCES gold.date_dim (dwh_date_key),
	FOREIGN KEY (hire_date) REFERENCES gold.date_dim (dwh_date_key),
	FOREIGN KEY (country_id) REFERENCES gold.country_dim (country_dim_key)
);

TRUNCATE gold.employees_dim;
MERGE INTO gold.employees_dim tgt
USING 
(SELECT employee_id,concat(first_name,' ',last_name) AS emp_name, title, dm.dwh_date_key AS birth_date, dmh.dwh_date_key AS hire_date, address, city,cd.country_dim_key AS country_id  ,region, postal_code, home_phone AS phone, "extension", COALESCE(reports_to,0) AS manager, valid_from, valid_to, valid_flag
FROM silver.sil_employees sc 
JOIN gold.country_dim cd 
ON sc.country=cd.country_name
JOIN gold.date_dim dm
ON sc.birth_date=dm.full_date
JOIN gold.date_dim dmh
ON sc.hire_date=dmh.full_date) as src
ON tgt.employee_id = src.employee_id
WHEN MATCHED AND (src.emp_name <> tgt.emp_name OR
				  src.title <> tgt.title OR
				  src.birth_date <> tgt.birth_date OR
				  src.hire_date <> tgt.hire_date OR
				  src.address <> tgt.address OR
				  src.city <> tgt.city OR
				  src.country_id <> tgt.country_id OR
				  src.region <> tgt.region OR
				  src.postal_code <> tgt.postal_code OR
				  src.phone <> tgt.phone OR
				  src."extension" <> tgt."extension" OR
				  src.manager <> tgt.manager 
				  )THEN 
UPDATE SET valid_to = CURRENT_DATE - INTERVAL '1 DAY', valid_flag = FALSE
WHEN NOT MATCHED THEN
INSERT (employee_id, emp_name, title, birth_date, hire_date, address, city, country_id, region, postal_code, phone, "extension", manager, valid_from, valid_to, valid_flag)
VALUES (src.employee_id, src.emp_name, src.title, src.birth_date, src.hire_date, src.address, src.city, src.country_id, src.region, src.postal_code, src.phone, src."extension",src.manager, src.valid_from, src.valid_to, src.valid_flag);

INSERT INTO gold.employees_dim (employee_id, emp_name, title, birth_date, hire_date, address, city, country_id, region, postal_code, phone, "extension", manager, valid_from, valid_to, valid_flag)
SELECT src.employee_id,CONCAT(src.first_name ,' ',src.last_name) AS emp_name, src.title, dm.dwh_date_key AS birth_date, dmh.dwh_date_key AS hire_date, src.address, src.city, cd.country_dim_key, src.region, src.postal_code, src.home_phone AS phone, src."extension",src.reports_to AS manager,
CURRENT_DATE,'2099-12-31',TRUE
FROM silver.sil_employees src 
JOIN gold.country_dim cd 
ON src.country=cd.country_name
JOIN gold.date_dim dm
ON src.birth_date=dm.full_date
JOIN gold.date_dim dmh
ON src.hire_date=dmh.full_date
JOIN gold.employees_dim tgt
ON src.employee_id=tgt.employee_id
AND tgt.valid_flag=FALSE
AND src.valid_flag =TRUE;

SELECT * FROM gold.employees_dim;

INSERT INTO gold.employees_dim(employee_id, emp_name)
VALUES(0,'NE');

------------------------------------------------------------------------------------------------------------------
--------------- Suppliers Dimension ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.suppliers_dim;
CREATE TABLE gold.suppliers_dim (
	dwh_sup_key serial PRIMARY KEY NOT NULL,
	supplier_id int2 NOT NULL,
	supplier_name varchar(40) NOT NULL,
	supplier_company varchar(40) NULL,
	supplier_title varchar(40) NULL,
	address varchar(60) NULL,
	city varchar(15) NULL,
	region varchar(15) NULL,
	country_id int NULL,
	postal_code varchar(10) NULL,
	phone varchar(24) NULL,
	fax varchar(24) NULL,
	valid_from date NULL,
	valid_to date NULL,
	valid_flag bool NULL,
	FOREIGN KEY (country_id) REFERENCES gold.country_dim (country_dim_key)
);

TRUNCATE gold.suppliers_dim;
MERGE INTO gold.suppliers_dim tgt
USING( 
SELECT supplier_id, company_name, contact_name, contact_title, address, city, region,
postal_code, cd.country_dim_key as country_id, phone, fax, valid_from, valid_to, valid_flag
FROM silver.sil_suppliers sup
JOIN gold.country_dim cd ON cd.country_name=sup.country) AS src
ON tgt.supplier_id = src.supplier_id
WHEN MATCHED AND (
	src.contact_name <> tgt.supplier_name OR
	src.company_name <> tgt.supplier_company OR
	src.contact_title <> tgt.supplier_title OR
	src.address <> tgt.address OR
	src.city <> tgt.city OR
	src.region <> src.region OR
	src.country_id <> tgt.country_id OR
	src.postal_code <> tgt.postal_code OR
	src.phone <> tgt.phone OR
	src.fax <> tgt.fax
)THEN
UPDATE
SET valid_to = CURRENT_DATE - INTERVAL '1 DAY', valid_flag = FALSE
WHEN NOT MATCHED THEN
INSERT (supplier_id, supplier_name, supplier_company, supplier_title, address, city, region, country_id, postal_code, phone, fax, valid_from, valid_to, valid_flag)
VALUES (src.supplier_id, src.contact_name,src.company_name,src.contact_title, src.address, src.city, src.region, src.country_id,src.postal_code, src.phone, src.fax, valid_from, valid_to, valid_flag);

INSERT INTO gold.suppliers_dim (supplier_id, supplier_name, supplier_company, supplier_title, address, city, region, country_id, postal_code, phone, fax, valid_from, valid_to, valid_flag)
SELECT src.supplier_id, src.company_name, src.contact_name, src.contact_title, src.address, src.city, src.region, cd.country_dim_key,src.postal_code, src.phone,src.fax,CURRENT_DATE,'2099-12-31',TRUE
FROM silver.sil_suppliers src
JOIN gold.country_dim cd
ON cd.country_name=src.country
JOIN gold.suppliers_dim tgt
ON src.supplier_id=tgt.supplier_id
AND tgt.valid_flag=FALSE;

SELECT * FROM gold.suppliers_dim;
------------------------------------------------------------------------------------------------------------------
--------------- Products Dimension ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.products_dim;
CREATE TABLE gold.products_dim (
	dwh_pro_key serial PRIMARY KEY NOT NULL,
	product_id int2 NOT NULL,
	product_name varchar(40) NOT NULL,
	supplier_id int2 NULL,
	quantity_per_unit varchar(20) NULL,
	unit_price float4 NULL,
	units_in_stock int2 NULL,
	units_on_order int2 NULL,
	reorder_level int2 NULL,
	discontinued int4  NULL,
	category_id int2  NULL,
	category_name varchar(15) NULL,
	valid_from date NULL,
	valid_to date NULL,
	valid_flag bool NULL,
	FOREIGN KEY (supplier_id) REFERENCES gold.suppliers_dim (dwh_sup_key)
);


TRUNCATE gold.products_dim;

MERGE INTO gold.products_dim tgt
USING 
(SELECT product_id,product_name,sp.supplier_id,quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued, COALESCE(sc.category_id,0) as category_id, COALESCE(sc.category_name,'No Category') as category_name,sp.valid_from, sp.valid_to, sp.valid_flag
FROM silver.sil_products sp
LEFT JOIN silver.sil_categories sc
ON sp.category_id=sc.category_id
JOIN gold.suppliers_dim sd
ON sd.supplier_id=sp.supplier_id
and sd.valid_flag=TRUE) as src
ON tgt.product_id = src.product_id
AND tgt.valid_flag=TRUE -- check this!!!
WHEN MATCHED AND (
       src.product_name <> tgt.product_name OR
       src.supplier_id <> tgt.supplier_id OR
       src.quantity_per_unit <> tgt.quantity_per_unit OR
       src.unit_price <> tgt.unit_price OR
       src.units_in_stock <> tgt.units_in_stock OR
       src.units_on_order <> tgt.units_on_order OR
       src.reorder_level <> tgt.reorder_level OR
       src.discontinued <> tgt.discontinued OR
       src.category_id <> tgt.category_id OR
       src.category_name <> tgt.category_name
)THEN
UPDATE SET valid_to = CURRENT_DATE - INTERVAL '1 DAY', valid_flag=FALSE
WHEN NOT MATCHED THEN
INSERT (product_id, product_name, supplier_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued, category_id, category_name, valid_from, valid_to, valid_flag)
VALUES (src.product_id, src.product_name, src.supplier_id, src.quantity_per_unit, src.unit_price, src.units_in_stock, src.units_on_order, src.reorder_level, src.discontinued, src.category_id, src.category_name, src.valid_from, src.valid_to,TRUE)

INSERT INTO gold.products_dim (product_id, product_name, supplier_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued, category_id, category_name, valid_from, valid_to, valid_flag)
SELECT src.product_id, src.product_name, src.supplier_id, src.quantity_per_unit, src.unit_price, src.units_in_stock, src.units_on_order, src.reorder_level, src.discontinued, src.category_id, src.category_name,CURRENT_DATE , '2099-12-31',TRUE 
FROM (SELECT product_id,product_name,sp.supplier_id,quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued, sc.category_id,sc.category_name,sp.valid_from, sp.valid_to, sp.valid_flag
FROM silver.sil_products sp
JOIN silver.sil_categories sc
ON sp.category_id=sc.category_id
JOIN gold.suppliers_dim sd
ON sd.supplier_id=sp.supplier_id
AND sd.valid_flag=TRUE) as src
JOIN gold.products_dim tgt
ON src.product_id=tgt.product_id
AND src.valid_flag=TRUE
AND tgt.valid_flag=FALSE;


SELECT src.product_id
FROM
(SELECT product_id,product_name,sp.supplier_id,quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued, sc.category_id,sc.category_name,sp.valid_from, sp.valid_to, sp.valid_flag
FROM silver.sil_products sp
JOIN silver.sil_categories sc
ON sp.category_id=sc.category_id
JOIN gold.suppliers_dim sd
ON sd.supplier_id=sp.supplier_id
and sd.valid_flag=TRUE) as src;

SELECT * FROM gold.products_dim;

INSERT INTO gold.products_dim (product_id, product_name)
VALUES( 0, 'NP');
------------------------------------------------------------------------------------------------------------------
--------------- Shippers Dimension ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.shippers_dim;
CREATE TABLE gold.shippers_dim (
	dwh_ship_key serial PRIMARY KEY NOT NULL,
	shipper_id int2 NOT NULL,
	company_name varchar(40) NOT NULL,
	phone varchar(24) NULL,
	valid_from date NULL,
	valid_to date NULL,
	valid_flag bool NULL
);

TRUNCATE gold.shippers_dim;
MERGE INTO gold.shippers_dim tgt
USING silver.sil_shippers src
ON tgt.shipper_id = src.shipper_id
WHEN MATCHED AND (src.company_name <> tgt.company_name OR src.phone <> tgt.phone) THEN 
UPDATE SET valid_to = CURRENT_DATE - INTERVAL '1 DAY', valid_flag = FALSE
WHEN NOT MATCHED THEN
INSERT (shipper_id, company_name, phone, valid_from, valid_to, valid_flag)
VALUES (src.shipper_id, src.company_name, src.phone, src.valid_from,'2099-12-31',TRUE);

INSERT INTO gold.shippers_dim (shipper_id, company_name, phone, valid_from, valid_to, valid_flag)
SELECT src.shipper_id, src.company_name, src.phone, CURRENT_DATE,'2099-12-31',TRUE
FROM silver.sil_shippers src
JOIN gold.shippers_dim tgt
ON src.shipper_id=tgt.shipper_id
AND tgt.valid_flag=FALSE;



SELECT * FROM gold.shippers_dim;
------------------------------------------------------------------------------------------------------------------
--------------- Shippers Dimension ------------------------------
------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.orders_fact;
CREATE TABLE gold.orders_fact ( -- add country
	dwh_order_key serial PRIMARY KEY NOT NULL,
	order_id int2 NOT NULL,
	customer_id int NOT NULL,
	employee_id int2 NULL,
	order_date int NOT NULL,
	required_date int  NULL,
	shipper_id int2  NULL,
	shipped_date int  NULL,
	shipping_cost float4  NULL,
	country_id int NULL,
	product_id int2  NULL,
	unit_price float4  NULL,
	quantity int2  NULL,
	discount float4  NULL,
	revenue float4  NULL,
	delivery_status varchar(15) NOT NULL,
	valid_from date,
	valid_to date,
	is_valid boolean
);

TRUNCATE gold.orders_fact;

MERGE INTO gold.orders_fact tgt
USING (SELECT so.order_id, cud.dwh_cust_key AS customer_id, COALESCE(ed.dwh_emp_key,0) AS employee_id, 
odm.dwh_date_key AS order_date, 
COALESCE(rdm.dwh_date_key,(SELECT dwh_date_key from gold.date_dim dd WHERE full_date = '2099-12-31')) AS required_date,
shd.dwh_ship_key AS shipper_id,
COALESCE(sdm.dwh_date_key,(SELECT dwh_date_key from gold.date_dim dd WHERE full_date = '2099-12-31')) AS shipped_date,
freight AS shipping_cost, COALESCE(pd.dwh_pro_key,0) AS product_id, COALESCE(sod.unit_price,0.0) AS unit_price, COALESCE(quantity,0) AS quantity, COALESCE(discount,0.0) AS discount,cod.country_dim_key as country_id,
COALESCE(round((sod.unit_price*quantity*(1-discount)):: numeric,2),0) AS revenue,
CASE WHEN so.shipped_date <= so.required_date THEN 'On Time'
WHEN so.shipped_date ='2099-12-31' THEN 'Not Delivered'
WHEN so.order_date ='2099-12-31' THEN 'Unvalid'
ELSE 'Delayed' END AS delivery_status,
so.valid_from, so.valid_to, so.valid_flag AS is_valid
FROM silver.sil_orders so
LEFT JOIN silver.sil_order_details sod
ON so.order_id=sod.order_id
LEFT JOIN gold.products_dim pd
ON sod.product_id=pd.product_id
LEFT JOIN gold.country_dim cod
ON cod.country_name=so.ship_country
LEFT JOIN gold.employees_dim ed
ON so.employee_id=ed.employee_id
AND ed.valid_flag=TRUE
JOIN gold.customers_dim cud
ON so.customer_id=cud.customer_id
AND cud.valid_flag=TRUE
JOIN gold.date_dim odm
ON so.order_date=odm.full_date
LEFT JOIN gold.date_dim sdm
ON so.shipped_date=sdm.full_date
JOIN gold.date_dim rdm
ON so.required_date=rdm.full_date
AND so.valid_flag=TRUE
JOIN gold.shippers_dim shd
ON so.ship_via=shd.shipper_id
AND shd.valid_flag=TRUE) as src
ON tgt.order_id = src.order_id
AND tgt.product_id = src.product_id
AND tgt.is_valid = TRUE
WHEN MATCHED AND (
	   src.customer_id <> tgt.customer_id OR
       src.employee_id <> tgt.employee_id OR
       src.order_date <> tgt.order_date OR
       src.required_date <> tgt.required_date OR
       src.shipper_id <> tgt.shipper_id OR
       src.shipped_date <> tgt.shipped_date OR
       src.shipping_cost <> tgt.shipping_cost OR
       src.product_id <> tgt.product_id OR
       src.unit_price <> tgt.unit_price OR
       src.quantity <> tgt.quantity OR
       src.discount <> tgt.discount
)THEN
UPDATE SET valid_to = CURRENT_DATE - INTERVAL '1 DAY', is_valid=FALSE
WHEN NOT MATCHED THEN
INSERT (order_id, customer_id, employee_id, order_date, required_date, shipper_id, shipped_date, shipping_cost, country_id, product_id, unit_price, quantity, discount, revenue, delivery_status,valid_from,valid_to,is_valid)
VALUES (src.order_id, src.customer_id, src.employee_id, src.order_date, src.required_date, src.shipper_id, src.shipped_date, src.shipping_cost, src.country_id,src.product_id, src.unit_price, src.quantity,src.discount,src.revenue,src.delivery_status,src.valid_from,'2099-12-31',TRUE)

--Distinct of join as join 3*3=9
INSERT INTO gold.orders_fact (order_id, customer_id, employee_id, order_date, required_date, shipper_id, shipped_date, shipping_cost,country_id, product_id, unit_price, quantity, discount, revenue, delivery_status,valid_from,valid_to,is_valid)
SELECT DISTINCT src.order_id, src.customer_id, COALESCE(src.employee_id,0), src.order_date, src.required_date, src.shipper_id, COALESCE (src.shipped_date,(SELECT dwh_date_key from gold.date_dim dd WHERE full_date = '2099-12-31')), src.shipping_cost,src.country_id, COALESCE(src.product_id,0),  COALESCE(src.unit_price,0), COALESCE (src.quantity,0.0),COALESCE (src.discount,0.0),COALESCE (src.revenue,0),src.delivery_status,CURRENT_DATE,'2099-12-31'::DATE,TRUE
FROM 
(SELECT so.order_id, cud.dwh_cust_key AS customer_id, ed.dwh_emp_key AS employee_id, odm.dwh_date_key AS order_date, rdm.dwh_date_key AS required_date,shd.dwh_ship_key AS shipper_id, sdm.dwh_date_key AS shipped_date,freight AS shipping_cost, pd.dwh_pro_key AS product_id, sod.unit_price, quantity, discount,cod.country_dim_key as country_id,
round((sod.unit_price*quantity*(1-discount)):: numeric,2) AS revenue,
CASE WHEN so.shipped_date <= so.required_date THEN 'On Time'
WHEN so.shipped_date ='2099-12-31' THEN 'Not Delivered'
WHEN so.order_date ='2099-12-31' THEN 'Unvalid' 
ELSE 'Delayed' END AS delivery_status,
so.valid_from,so.valid_to,so.valid_flag 
FROM silver.sil_orders so
LEFT JOIN silver.sil_order_details sod
ON so.order_id=sod.order_id
LEFT JOIN gold.products_dim pd
ON sod.product_id=pd.product_id
LEFT JOIN gold.country_dim cod
ON cod.country_name=so.ship_country
LEFT JOIN gold.employees_dim ed
ON so.employee_id=ed.employee_id
AND ed.valid_flag=TRUE
JOIN gold.customers_dim cud
ON so.customer_id=cud.customer_id
AND cud.valid_flag=TRUE
JOIN gold.date_dim odm
ON so.order_date=odm.full_date
LEFT JOIN gold.date_dim sdm
ON so.shipped_date=sdm.full_date
JOIN gold.date_dim rdm
ON so.required_date=rdm.full_date
AND so.valid_flag=TRUE
JOIN gold.shippers_dim shd
ON so.ship_via=shd.shipper_id
AND shd.valid_flag=TRUE
WHERE so.valid_from=current_date-1
) as src
LEFT JOIN gold.orders_fact tgt 
ON tgt.order_id=src.order_id
AND tgt.is_valid=TRUE;


SELECT * FROM gold.orders_fact of2 ;


