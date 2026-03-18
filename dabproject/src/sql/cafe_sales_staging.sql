DECLARE VARIABLE catalog_name STRING;
DECLARE VARIABLE bronze_schema_name STRING;
DECLARE VARIABLE silver_schema_name STRING;


SET VAR catalog_name = TRIM(:catalog_name);
SET VAR bronze_schema_name = TRIM(:bronze_schema_name);
SET VAR silver_schema_name = TRIM(:silver_schema_name);

USE CATALOG IDENTIFIER(catalog_name);
USE SCHEMA IDENTIFIER(silver_schema_name);

TRUNCATE TABLE IDENTIFIER(catalog_name || '.' || silver_schema_name || '.' || 'cafe_sales_staging');
INSERT INTO IDENTIFIER(catalog_name || '.' || silver_schema_name || '.' || 'cafe_sales_staging')

WITH transformed AS (

SELECT 
    TRIM(`Transaction ID`) AS TRANSACTION_ID,
    TRIM(`Item`) AS ITEM,
    TRY_CAST(`Quantity` AS INT) AS QUANTITY,
    TRY_CAST(`Price Per Unit` AS DOUBLE) AS PRICE_PER_UNIT,
    TRY_CAST(`Total Spent` AS DOUBLE) AS TOTAL_SPENT,
    TRIM(`Payment Method`) AS PAYMENT_METHOD,
    TRIM(`Location`) AS LOCATION,
    TRY_TO_DATE(TRIM(`Transaction Date`), 'yyyy-MM-dd') AS TRANSACTION_DATE
FROM IDENTIFIER(catalog_name || '.' || bronze_schema_name || '.' || 'cafe_sales_raw')
),

cleaned AS (
    SELECT *
    FROM transformed
    WHERE 
        ITEM IS NOT NULL
        AND QUANTITY IS NOT NULL
        AND PRICE_PER_UNIT IS NOT NULL
        AND TOTAL_SPENT IS NOT NULL
        AND PAYMENT_METHOD IS NOT NULL
        AND LOCATION IS NOT NULL
        AND TRANSACTION_DATE IS NOT NULL
)

SELECT * FROM cleaned;
