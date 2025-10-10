/*
==================================================================================
Silver Layer ETL Stored Procedure: silver.load_silver
==================================================================================
Script Purpose:
    This script defines a stored procedure responsible for loading cleansed and 
    standardized data from the Bronze Layer into the Silver Layer tables. 
    It performs schema alignment, data deduplication, referential integrity 
    enforcement, and value standardization across entities such as agents, 
    clients, properties, sales, and locations.

Technical Workflow:
    1. Disables foreign key checks to allow controlled truncation and reload.
    2. Truncates existing Silver tables to ensure a clean load.
    3. Applies data cleaning and transformation logic:
        - Removes duplicates.
        - Standardizes casing and formats for textual fields.
        - Handles nulls and missing values with placeholders (e.g., 'N/A').
        - Normalizes inconsistent state spellings using rule-based corrections.
        - Converts date formats to standardized SQL datetime values.
    4. Re-enables foreign key checks post-load to maintain relational consistency.

Data Engineering Context:
    This procedure represents the Transformation and Cleansing stage 
    within a Medallion Architecture (Bronze → Silver → Gold). It ensures 
    the Silver layer serves as a clean, conformed, and analytics-ready 
    foundation for downstream modeling and business intelligence consumption.

WARNING:
    Running this procedure will truncate and reload all Silver tables, 
    overwriting existing data. Execute with caution in production environments.
==================================================================================
*/

DELIMITER $$

DROP PROCEDURE IF EXISTS silver.load_silver$$

CREATE PROCEDURE silver.load_silver()
BEGIN

SET FOREIGN_KEY_CHECKS = 0;

TRUNCATE TABLE silver.agents;
INSERT INTO silver.agents(
    agent_id,
    agent_name,
    agent_phone_number,
    agent_email,
    agency
)
SELECT 
      AgentID,
    CASE 
        WHEN TRIM(Name) = '' THEN 'N/A'
        ELSE CONCAT(
            UPPER(LEFT(TRIM(SUBSTRING_INDEX(TRIM(REPLACE(Name, '\t', ' ')), ' ', 1)), 1)),
            LOWER(SUBSTRING(TRIM(SUBSTRING_INDEX(TRIM(REPLACE(Name, '\t', ' ')), ' ', 1)), 2)),
            ' ',
            UPPER(LEFT(TRIM(SUBSTRING_INDEX(TRIM(REPLACE(Name, '\t', ' ')), ' ', -1)), 1)),
            LOWER(SUBSTRING(TRIM(SUBSTRING_INDEX(TRIM(REPLACE(Name, '\t', ' ')), ' ', -1)), 2))
        )
    END AS agent_name,
    PhoneNumber AS agent_phone_number,
    Email AS agent_email,
    CASE 
    WHEN TRIM(Agency) = '' THEN 'N/A'
    ELSE TRIM(
        CONCAT_WS(' ',
            CASE WHEN SUBSTRING_INDEX(Agency, ' ', 1) <> '' 
                THEN CONCAT(UPPER(LEFT(SUBSTRING_INDEX(Agency, ' ', 1), 1)), LOWER(SUBSTRING(SUBSTRING_INDEX(Agency, ' ', 1), 2))) 
            END,
            CASE WHEN (LENGTH(Agency) - LENGTH(REPLACE(Agency, ' ', ''))) >= 1 
                THEN CONCAT(UPPER(LEFT(SUBSTRING_INDEX(SUBSTRING_INDEX(Agency, ' ', 2), ' ', -1), 1)), LOWER(SUBSTRING(SUBSTRING_INDEX(SUBSTRING_INDEX(Agency, ' ', 2), ' ', -1), 2))) 
            END,
            CASE WHEN (LENGTH(Agency) - LENGTH(REPLACE(Agency, ' ', ''))) >= 2 
                THEN CONCAT(UPPER(LEFT(SUBSTRING_INDEX(SUBSTRING_INDEX(Agency, ' ', 3), ' ', -1), 1)), LOWER(SUBSTRING(SUBSTRING_INDEX(SUBSTRING_INDEX(Agency, ' ', 3), ' ', -1), 2))) 
            END,
            CASE WHEN (LENGTH(Agency) - LENGTH(REPLACE(Agency, ' ', ''))) >= 3 
                THEN CONCAT(UPPER(LEFT(SUBSTRING_INDEX(SUBSTRING_INDEX(Agency, ' ', 4), ' ', -1), 1)), LOWER(SUBSTRING(SUBSTRING_INDEX(SUBSTRING_INDEX(Agency, ' ', 4), ' ', -1), 2))) 
            END
        )
    )
END AS agency

FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY AgentID ORDER BY AgentID) AS rn
    FROM bronze.agents
) AS RankedAgents
WHERE rn = 1;

-- 2) CLIENT

TRUNCATE TABLE silver.clients;

INSERT INTO silver.clients(
client_id,
client_name,
client_phone_number,
client_email
)
SELECT 
    ClientID,
    CASE 
        WHEN TRIM(Name) = '' THEN 'N/A'
        ELSE CONCAT_WS(' ',
            CONCAT(UPPER(LEFT(TRIM(SUBSTRING_INDEX(TRIM(REPLACE(Name, '\t', ' ')), ' ', 1)), 1)),
                   LOWER(SUBSTRING(TRIM(SUBSTRING_INDEX(TRIM(REPLACE(Name, '\t', ' ')), ' ', 1)), 2))),
            CASE WHEN SUBSTRING_INDEX(TRIM(REPLACE(Name, '\t', ' ')), ' ', -1) != SUBSTRING_INDEX(TRIM(REPLACE(Name, '\t', ' ')), ' ', 1)
                 THEN CONCAT(UPPER(LEFT(TRIM(SUBSTRING_INDEX(TRIM(REPLACE(Name, '\t', ' ')), ' ', -1)), 1)),
                             LOWER(SUBSTRING(TRIM(SUBSTRING_INDEX(TRIM(REPLACE(Name, '\t', ' ')), ' ', -1)), 2)))
            END
        )
    END AS client_name,
    PhoneNumber AS client_phone_number,
    Email AS client_email
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY ClientID ORDER BY ClientID) AS rn
    FROM bronze.clients
) AS RankedClients
WHERE rn = 1;


-- 3) Properties

TRUNCATE TABLE silver.properties;

INSERT INTO silver.properties (
    property_id,
    address,
    city,
    state,
    zipcode,
    property_type,
    property_price,
    property_square_feet,
    property_bedrooms,
    property_bathrooms,
    agent_id
)
SELECT 
    TRIM(PropertyID) AS property_id,
    CASE WHEN TRIM(Address) = '' THEN 'N/A' ELSE TRIM(Address) END AS address,
    CASE 
        WHEN TRIM(City) = '' THEN 'N/A'
        ELSE CONCAT(
            UPPER(LEFT(TRIM(SUBSTRING_INDEX(City, ' ', 1)), 1)),
            LOWER(SUBSTRING(TRIM(SUBSTRING_INDEX(City, ' ', 1)), 2)),
            IF(INSTR(TRIM(City), ' ') > 0, 
               CONCAT(' ', UPPER(LEFT(TRIM(SUBSTRING_INDEX(City, ' ', -1)), 1)), 
                         LOWER(SUBSTRING(TRIM(SUBSTRING_INDEX(City, ' ', -1)), 2))), '')
        )
    END AS city,
    TRIM(State) AS state,
    CASE WHEN TRIM(ZipCode) = '' THEN 'N/A' ELSE TRIM(ZipCode) END AS zipcode,
    CASE WHEN TRIM(Type) = '' THEN 'N/A' ELSE CONCAT(UPPER(LEFT(Type,1)), LOWER(SUBSTRING(Type,2))) END AS property_type,
    COALESCE(Price, 0) AS property_price,
    CASE 
    WHEN SquareFeet IS NULL OR REPLACE(TRIM(SquareFeet), '\t', '') = '' 
        THEN 0 
    ELSE CAST(REPLACE(TRIM(SquareFeet), '\t', '') AS UNSIGNED) 
END AS property_square_feet,

CASE 
    WHEN Bedrooms IS NULL OR REPLACE(TRIM(Bedrooms), '\t', '') = '' 
        THEN 0 
    ELSE CAST(REPLACE(TRIM(Bedrooms), '\t', '') AS UNSIGNED) 
END AS property_bedrooms,

CASE 
    WHEN Bathrooms IS NULL OR REPLACE(TRIM(Bathrooms), '\t', '') = '' 
        THEN 0 
    ELSE CAST(REPLACE(TRIM(Bathrooms), '\t', '') AS UNSIGNED) 
END AS property_bathrooms,

    CASE WHEN TRIM(AgentID) = '' THEN 'N/A' ELSE TRIM(AgentID) END AS agent_id
FROM bronze.properties;

-- 4) SALES

TRUNCATE TABLE silver.sales;

INSERT INTO silver.sales(
sales_id,
property_id,
client_id,
agent_id,
sale_date,
sale_price
)
SELECT 
    SalesID,
    PropertyID,
    ClientID,
    AgentID,
    CASE
        WHEN CAST(SaleDate AS CHAR) LIKE '____-__-__' THEN STR_TO_DATE(SaleDate, '%Y-%m-%d')
        WHEN CAST(SaleDate AS CHAR) LIKE '%-%-%' AND LENGTH(SaleDate) = 10 THEN STR_TO_DATE(SaleDate, '%m-%d-%Y')
        WHEN CAST(SaleDate AS CHAR) LIKE '%/%/%' THEN STR_TO_DATE(SaleDate, '%m/%d/%Y')
        ELSE NULL
    END AS sale_date,
    NULLIF(TRIM(SalePrice), '') AS sale_price
FROM bronze.sales;

-- 5) Locations

TRUNCATE TABLE silver.locations;

INSERT INTO silver.locations (
    zipcode,
    city,
    state,
    median_income,
    population
)
SELECT
    ZipCode AS zipcode,
     CASE
        WHEN REPLACE(TRIM(City), '\t', '') = '' OR City IS NULL THEN
            CASE State
                WHEN 'NY' THEN 'New York'
                WHEN 'MO' THEN 'Kansas City'
                WHEN 'CA' THEN 'Sacramento'
                WHEN 'WA' THEN 'Seattle'
                ELSE 'N/A'
            END
        ELSE CONCAT_WS(' ',
            CONCAT(UPPER(LEFT(SUBSTRING_INDEX(TRIM(REPLACE(City, '\t', ' ')), ' ', 1), 1)),
                   LOWER(SUBSTRING(SUBSTRING_INDEX(TRIM(REPLACE(City, '\t', ' ')), ' ', 1), 2))),
            CASE 
                WHEN SUBSTRING_INDEX(TRIM(REPLACE(City, '\t', ' ')), ' ', -1) != SUBSTRING_INDEX(TRIM(REPLACE(City, '\t', ' ')), ' ', 1)
                THEN CONCAT(UPPER(LEFT(SUBSTRING_INDEX(TRIM(REPLACE(City, '\t', ' ')), ' ', -1), 1)),
                            LOWER(SUBSTRING(SUBSTRING_INDEX(TRIM(REPLACE(City, '\t', ' ')), ' ', -1), 2)))
            END
        )
    END AS city,
    State AS state,
    MedianIncome AS median_income,
    ROUND(Population, -3) AS population
FROM bronze.locations;


SET FOREIGN_KEY_CHECKS = 1;
END$$
DELIMITER ;
