

WITH source_data AS (
    SELECT *
    FROM {{ source('northwind', 'order_details') }}
)
SELECT
orderid AS order_id,
productid AS product_id,
unitprice::NUMERIC AS unit_price,
quantity::INT AS quantity,
discount::NUMERIC AS discount
FROM northwind.order_details od 




