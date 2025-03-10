
--------------------------------------------------------------------------------------------------------------
                                             --- KPI's--
--------------------------------------------------------------------------------------------------------------
SELECT round((sum(revenue)/count(distinct order_id))::NUMERIC ,2) As "Average Order Value" 
FROM gold.orders_fact of2
WHERE is_valid =TRUE;	
---------------------------------------------------------------------------------------------------------------
SELECT sum(revenue) AS "Customer Lifetime Value (CLV)", customer_id 
FROM gold.orders_fact of2 
WHERE is_valid =TRUE
GROUP BY customer_id 
ORDER BY "Customer Lifetime Value (CLV)" DESC;
---------------------------------------------------------------------------------------------------------------
WITH onTimeorders AS(
SELECT count(DISTINCT order_id) AS On_Time
FROM gold.orders_fact of2 
WHERE delivery_status = 'On Time'
AND is_valid = TRUE
), DeliveredOrders AS(
SELECT count(DISTINCT order_id) AS Delivered_Orders
FROM gold.orders_fact of2 
WHERE delivery_status <> 'Not Delivered'
AND is_valid = TRUE
)
SELECT round((ot.On_Time::NUMERIC/dos.Delivered_Orders::NUMERIC),2)*100 AS "ON Time Delivey Rate"
FROM onTimeorders ot,
	 DeliveredOrders dos;
----------------------------------------------------------------------------------------------------------------
SELECT round(sum(dd.full_date-dd2.full_date)::NUMERIC / count(DISTINCT order_id),2) AS "Average Delivery Delay"
FROM gold.orders_fact of2 
JOIN gold.date_dim dd
ON of2.shipped_date =dd.dwh_date_key 
JOIN gold.date_dim dd2 
ON of2.required_date =dd2.dwh_date_key 
WHERE delivery_status = 'Delayed'
AND is_valid = TRUE;
----------------------------------------------------------------------------------------------------------------
SELECT cd.country_name AS Country_Name ,sum(revenue) AS "Revenue By Country"
FROM gold.orders_fact of2
JOIN gold.country_dim cd 
ON of2.country_id =cd.country_dim_key 
AND of2.is_valid =TRUE
GROUP by Country_Name;
----------------------------------------------------------------------------------------------------------------
SELECT pd.product_name AS Product_Name,sum(revenue) "Revenue By Product"
FROM gold.orders_fact of2
JOIN gold.products_dim pd 
ON of2.product_id =pd.dwh_pro_key 
AND of2.is_valid =TRUE 
AND pd.valid_flag=TRUE
GROUP by Product_Name;
-----------------------------------------------------------------------------------------------------------------
WITH MonthlyRevenue AS (
	SELECT sum(revenue) AS Revenue , dd."year" AS "YEAR",TO_CHAR(dd.full_date,'MM') AS Month_in_numbers,dd."month" AS "Month",
	ROW_NUMBER() OVER (ORDER BY dd."year" DESC, TO_CHAR(dd.full_date,'MM') DESC) AS Row_Num
    FROM gold.orders_fact ofc
    JOIN gold.date_dim dd
    ON ofc.order_date = dd.dwh_date_key 
    GROUP BY "YEAR",Month_in_numbers, "Month"
    ORDER BY "YEAR" DESC, Month_in_numbers DESC
),
CurrentMonthRevenue AS (
	SELECT Revenue, "Month" AS Current_Month
    FROM MonthlyRevenue
    WHERE "YEAR" = (SELECT MAX("YEAR") FROM MonthlyRevenue)
    AND "Month" = TO_CHAR(CURRENT_DATE, 'Month')
),
PreviousMonthRevenue AS (
	SELECT Revenue , "Month" AS Previous_Month
    FROM MonthlyRevenue
    WHERE Row_Num = 2
)
SELECT CONCAT(round((((cm.Revenue - pm.Revenue) / pm.Revenue)*100)::NUMERIC,2) , '%') AS Monthly_Growth_Rate
FROM 
    PreviousMonthRevenue pm,
    CurrentMonthRevenue cm;
--------------------------------------------------------------------------------------------------------------------
WITH YearlyRevenue AS (
	SELECT sum(revenue) AS Revenue, dd."year" AS "YEAR"
    FROM gold.orders_fact ofc
    JOIN gold.date_dim dd
    ON ofc.order_date = dd.dwh_date_key 
    GROUP BY "YEAR"
    ORDER BY "YEAR" DESC
),
CurrentYearRevenue AS (
	SELECT Revenue, "YEAR" AS Current_Year
    FROM YearlyRevenue
    WHERE "YEAR" = (SELECT MAX("YEAR") FROM YearlyRevenue)
),
PreviousYearRevenue AS (
  SELECT COALESCE(Revenue, 0) AS Revenue, "YEAR" AS Previous_Year
    FROM YearlyRevenue
    WHERE "YEAR" < (SELECT MAX("YEAR") FROM YearlyRevenue)
    LIMIT 1
)
SELECT CONCAT(round((((cy.Revenue - py.Revenue) / py.Revenue)*100)::NUMERIC,2),'%') AS Yearly_Growth_Rate
FROM 
    PreviousYearRevenue py,
    CurrentYearRevenue cy;
-------------------------------------------------------------------------------------------------------------
WITH RFM_Scores AS (
    SELECT cd.customer_id AS Customer,
	EXTRACT(DAY FROM NOW() - MAX(dd.full_date)) AS Recency,
	COUNT(of2.order_id) AS Frequency,
	SUM(of2.revenue) AS Monetary
    FROM gold.customers_dim cd
    JOIN gold.orders_fact of2
    ON cd.dwh_cust_key = of2.customer_id
    AND cd.valid_flag = TRUE
    JOIN gold.date_dim dd 
    ON dd.dwh_date_key=of2.order_date
    AND of2.is_valid= TRUE
    GROUP BY cd.customer_id
),
RFM_Limits AS(
	SELECT CEIL(AVG(Frequency)) AS avg_freq,
	CEIL(AVG(Monetary)) AS avg_money,
	CEIL(AVG(Recency)) AS avg_days
	FROM RFM_Scores
),
CustomerSegments AS (
    SELECT Customer,
        CASE
            WHEN RFM.Recency <= LM.avg_days AND RFM.Frequency >= LM.avg_freq AND RFM.monetary >= LM.avg_money THEN 'Loyal Top Spenders'
            WHEN RFM.recency > LM.avg_days AND RFM.Frequency >= LM.avg_freq AND RFM.monetary < LM.avg_money THEN 'Frequent Buyers'
            WHEN RFM.recency > LM.avg_days AND RFM.Frequency < LM.avg_freq AND RFM.monetary < LM.avg_money THEN 'Occasional Buyers'
            WHEN RFM.recency > LM.avg_days THEN 'Dormant Customers'
            ELSE 'Other'
        END AS SegmentName
    FROM
        RFM_Scores AS RFM,
        RFM_Limits AS LM
         
) MERGE INTO gold.custsegment_dim tgt
  USING CustomerSegments src
  ON tgt.CustomerID=src.Customer
  WHEN MATCHED THEN 
  UPDATE SET CustomerID=src.Customer,
  		 	 SegmentName=src.SegmentName
  WHEN NOT MATCHED THEN
  INSERT (CustomerID, SegmentName)
  VALUES (src.Customer, src.SegmentName)

DROP TABLE IF EXISTS gold.custsegment_dim;
CREATE TABLE gold.custsegment_dim(
	dwh_custseg_key serial PRIMARY KEY,
	CustomerID varchar(10),
	SegmentName varchar(50)
);

SELECT * FROM gold.custsegment_dim cd;
------------------------------------------------------------------------------------------------------------------
SELECT csd.segmentname AS Customer_Segment,sum(revenue) "Revenue By Customer Segment"
FROM gold.orders_fact of2
JOIN gold.customers_dim cd 
ON of2.customer_id =cd.dwh_cust_key 
JOIN gold.custsegment_dim csd
ON cd.customer_id =csd.customerid 
AND of2.is_valid =TRUE 
AND cd.valid_flag=TRUE
GROUP by Customer_Segment;
------------------------------------------------------------------------------------------------------------------
-- Churn Customers
WITH Customers_Orders AS (
SELECT cd.customer_id AS Customer,dd.full_date AS Order_date
FROM gold.orders_fact of2
LEFT JOIN gold.customers_dim cd 
ON of2.customer_id =cd.dwh_cust_key 
JOIN gold.date_dim dd 
ON dd.dwh_date_key =of2.order_date
AND of2.is_valid =TRUE 
AND cd.valid_flag=TRUE
GROUP BY dd.full_date,Customer
),
Last_Order_Date AS (
SELECT Customer, Max(order_date) AS Last_order
FROM Customers_Orders
GROUP BY Customer
),
Churned_Customers AS (
SELECT DISTINCT (Customer),CASE WHEN Last_Order <= (current_date - INTERVAL '12 months')::DATE THEN 'Churned'
ELSE 'Active' END AS Churn_Status
FROM Last_Order_Date
) 
SELECT CONCAT(Round((COUNT(DISTINCT c.Customer) * 100.0) / (SELECT COUNT(*) FROM gold.customers_dim WHERE valid_flag = TRUE),2),'%') AS churn_rate_percentage
FROM Churned_Customers c
WHERE Churn_Status='Churned';




