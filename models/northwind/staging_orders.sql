
WITH source_data AS (
    SELECT *
    FROM {{ source('northwind', 'orders') }}
)
SELECT
orderid AS order_id,
customerid AS customer_id,
employeeid AS employee_id,
orderdate::DATE AS order_date,
requireddate::DATE AS required_date,
shippeddate::DATE AS shipped_date,
shipvia AS ship_via,
freight AS freight,
shipname AS ship_name,
shipaddress AS ship_address,
shipcity AS ship_city,
shipregion AS ship_region,
shippostalcode AS ship_postal_code,
shipcountry AS ship_country
FROM northwind.orders o;