
/*
SELECT *
FROM northwind.order_details od,
 */

/*
Use {{ source() }} to pull data from the raw tables.
Rename columns to snake_case.
Cast dates and numbers to the correct types.
Keep only relevant columns.
 */

/*
SELECT *
FROM northwind.order_details o
WHERE unitprice::NUMERIC != FLOOR(unitprice::NUMERIC);
*/

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




