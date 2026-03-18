DECLARE VARIABLE catalog_name STRING;
DECLARE VARIABLE silver_schema_name STRING;
DECLARE VARIABLE gold_schema_name STRING;


SET VAR catalog_name = TRIM(:catalog_name);
SET VAR silver_schema_name = TRIM(:silver_schema_name);
SET VAR gold_schema_name = TRIM(:gold_schema_name);

USE CATALOG IDENTIFIER(catalog_name);
USE SCHEMA IDENTIFIER(gold_schema_name);

TRUNCATE TABLE IDENTIFIER(catalog_name || '.' || gold_schema_name || '.' || 'cafe_sales_hub');
INSERT INTO IDENTIFIER(catalog_name || '.' || gold_schema_name || '.' || 'cafe_sales_hub')

WITH item_totals AS (
    SELECT
        ITEM,
        SUM(TOTAL_SPENT) AS TOTAL_REVENUE,
        SUM(QUANTITY) AS TOTAL_QUANTITY
    FROM dev_lakehouse.silver.cafe_sales_staging
    GROUP BY ITEM
),

ranked_items AS (
    SELECT *,
           RANK() OVER (ORDER BY TOTAL_REVENUE DESC) AS REVENUE_RANK,
           RANK() OVER (ORDER BY TOTAL_QUANTITY DESC) AS QUANTITY_RANK
    FROM item_totals
)

SELECT
    s.*,

    -- Monthly Revenue per Item
    SUM(s.TOTAL_SPENT) OVER (
        PARTITION BY s.ITEM,
        DATE_TRUNC('month', s.TRANSACTION_DATE)
    ) AS MONTHLY_REVENUE_PER_ITEM,

    -- Total Quantity Sold per Item
    SUM(s.QUANTITY) OVER (
        PARTITION BY s.ITEM
    ) AS TOTAL_QUANTITY_SOLD_PER_ITEM,

    -- Revenue by Payment Method
    SUM(s.TOTAL_SPENT) OVER (
        PARTITION BY s.PAYMENT_METHOD
    ) AS REVENUE_BY_PAYMENT_METHOD,

    -- Overall Total Revenue
    SUM(s.TOTAL_SPENT) OVER () AS OVERALL_TOTAL_REVENUE,

    -- Revenue Rank
    r.REVENUE_RANK,

    -- Quantity Rank
    r.QUANTITY_RANK

FROM IDENTIFIER(catalog_name || '.' || silver_schema_name || '.' || 'cafe_sales_staging') s
LEFT JOIN ranked_items r
    ON s.ITEM = r.ITEM;


    
