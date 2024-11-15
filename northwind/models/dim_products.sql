WITH stg_products AS (
    SELECT * FROM {{ source('northwind', 'Products') }}
),
stg_suppliers AS (
    SELECT * FROM {{ source('northwind', 'Suppliers') }}
),
stg_categories AS (
    SELECT * FROM {{ source('northwind', 'Categories') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['p.PRODUCTID']) }} AS productkey,
    p.PRODUCTID,
    p.PRODUCTNAME,
    s.SUPPLIERID AS supplierkey,
    c.CATEGORYNAME,
    c.DESCRIPTION AS categorydescription
FROM stg_products p
LEFT JOIN stg_suppliers s ON p.SUPPLIERID = s.SUPPLIERID
LEFT JOIN stg_categories c ON p.CATEGORYID = c.CATEGORYID





