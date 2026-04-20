/*
Use {{ source() }} to pull data from the raw tables.
Rename columns to snake_case.
Cast dates and numbers to the correct types.
Keep only relevant columns.
 */

/*
SELECT *
FROM northwind.categories c;
*/

WITH source_data AS (
    SELECT *
    FROM {{ source('northwind', 'categories') }}
)
SELECT *
FROM northwind.categories c;

SELECT
categoryid AS category_id,
categoryname AS category_name,
description AS description
-- picture AS picture
FROM northwind.categories c;



