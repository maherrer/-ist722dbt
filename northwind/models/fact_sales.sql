WITH orders AS (
    SELECT * FROM {{ source('northwind', 'Orders') }}
),
order_details AS (
    SELECT * FROM {{ source('northwind', 'Order_Details') }}
),
dim_customer AS (
    SELECT CUSTOMERID, CUSTOMERKEY FROM {{ ref('dim_customer') }}
),
dim_employee AS (
    SELECT EMPLOYEEID, EMPLOYEEKEY FROM {{ ref('dim_employee') }}
),
dim_products AS (
    SELECT PRODUCTID, PRODUCTKEY FROM {{ ref('dim_products') }}
),
dim_date AS (
    SELECT 
        DATEKEY,
        DATE,
        YEAR,
        MONTH,
        QUARTER,
        DAY, 
        DAYOFWEEK,
        WEEKOFYEAR,
        DAYOFYEAR,
        QUARTERNAME,
        MONTHNAME,
        DAYNAME,
        WEEKDAY
    FROM {{ ref('dim_date') }}
)

SELECT 
    o.ORDERID,
    dc.CUSTOMERKEY,
    de.EMPLOYEEKEY,
    dd.DATEKEY AS ORDERDATEKEY,
    dp.PRODUCTKEY,
    od.QUANTITY,
    od.QUANTITY * od.UNITPRICE AS EXTENDEDPRICEAMOUNT,
    (od.QUANTITY * od.UNITPRICE) * od.DISCOUNT AS DISCOUNTAMOUNT,
    (od.QUANTITY * od.UNITPRICE) - ((od.QUANTITY * od.UNITPRICE) * od.DISCOUNT) AS SOLDAMOUNT
FROM orders o
JOIN order_details od ON o.ORDERID = od.ORDERID
LEFT JOIN dim_customer dc ON o.CUSTOMERID = dc.CUSTOMERID
LEFT JOIN dim_employee de ON o.EMPLOYEEID = de.EMPLOYEEID
LEFT JOIN dim_products dp ON od.PRODUCTID = dp.PRODUCTID
LEFT JOIN dim_date dd ON o.ORDERDATE = dd.DATE






