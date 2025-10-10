/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

USE silver;

SELECT * FROM silver.agents;
-- Checking for Duplicate vales

SELECT agent_id, COUNT(*) AS DuplicateCount
FROM silver.agents
GROUP BY agent_id
HAVING COUNT(*) > 1;

-- Checking for unwanted spaces
SELECT * 
FROM silver.agents
WHERE agent_name <> trim(agent_name);


-- Standardization & Consistency
-- Checking for NULL and Blank Values
SELECT Count(*)
FROM silver.agents
WHERE agent_id IS NULL
OR agent_name IS NULL
OR agency IS NULL;

SELECT *
FROM silver.agents
WHERE agent_id IS NULL
   OR agent_name IS NULL
   OR agency IS NULL;
   
SELECT Count(*)
FROM silver.agents
WHERE agent_name = '' OR agency = '';

-- 2) Client

SELECT * FROM silver.clients;

-- Checking for Duplicate vales

SELECT client_id, COUNT(*) AS DuplicateCount
FROM silver.clients
GROUP BY client_id
HAVING COUNT(*) > 1;


-- Checking for unwanted spaces

SELECT client_id, client_name 
FROM silver.clients
WHERE client_name <> TRIM(client_name);

-- Standardization & Consistency
-- Checking for NULL Values

SELECT *
FROM silver.clients
WHERE client_id IS NULL
OR client_name IS NULL;

SELECT COUNT(*)
FROM silver.clients
WHERE client_name ='';

-- 3) Properties

SELECT * FROM silver.properties;

-- Checking for Duplicate values

SELECT property_id, COUNT(*) AS DuplicateCount
FROM silver.properties
GROUP BY property_id
HAVING COUNT(*) > 1;


-- Checking for unwanted spaces

SELECT property_id, city
FROM silver.properties
WHERE city <> TRIM(city);
-- Standardization & Consistency
-- Checking for NULL Values

SELECT *
FROM silver.properties
WHERE property_id IS NULL
OR city IS NULL;

SELECT COUNT(*)
FROM silver.properties
WHERE TRIM(property_id) = 'N/A'
   OR TRIM(address) = 'N/A'
   OR TRIM(city) = 'N/A'
   OR TRIM(state) = 'N/A'
   OR TRIM(zipcode) = 'N/A'
   OR TRIM(property_type) = 'N/A'
   OR property_price = 0
   OR property_square_feet = 0
   OR property_bedrooms = 0
   OR property_bathrooms = 0
   OR TRIM(agent_id) = 'N/A';



SELECT *
FROM silver.properties
WHERE property_id =''
OR address =''
OR city =''
OR state = ''
OR zipcode =''
OR property_type =''
OR property_price =''
OR property_square_feet =''
OR property_bedrooms =''
OR property_bathrooms =''
OR agent_id ='';

SELECT DISTINCT State 
FROM silver.properties
ORDER BY State;

-- 4) Sales

-- Checking for Duplicate vales
SELECT * FROM silver.sales;

SELECT sales_id, COUNT(*) AS DuplicateCount
FROM silver.sales
GROUP BY sales_id
HAVING COUNT(*) > 1;


-- Standardization & Consistency
-- Checking for NULL Values
SELECT *
FROM silver.sales
WHERE sales_id IS NULL
OR property_id IS NULL
OR client_id IS NULL
OR sale_date IS NULL
OR sale_price IS NULL;

SELECT Count(*)
FROM silver.sales
WHERE sales_id = '' OR property_id = '' OR client_id = ''
OR sale_date = '' OR sale_price = '';

-- Checking for invalid dates

SELECT sale_id, sale_date
FROM silver.sales
WHERE 
    -- Not valid YYYY-MM-DD
    NOT (sale_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
    OR sale_date IS NULL
    OR sale_date = '';
    
-- Checking for future dates

  SELECT sales_id, sale_date,
       STR_TO_DATE(sale_date, '%Y-%m-%d') AS ParsedDate
FROM silver.sales
WHERE (
        STR_TO_DATE(sale_date, '%Y-%m-%d') > CURDATE()
     OR STR_TO_DATE(sale_date, '%m-%d-%Y') > CURDATE()
     OR STR_TO_DATE(sale_date, '%d/%m/%Y') > CURDATE()
);
-- Checking for 0, NULL or Blank values in Sales Price
SELECT sales_id, sale_price
FROM silver.sales
WHERE sale_price<= 0 OR sale_price IS NULL OR sale_price = '';

-- 5) Locations

SELECT * FROM silver.locations;

-- Checking for Duplicate vales

SELECT zipcode, COUNT(*) AS DuplicateCount
FROM silver.locations
GROUP BY zipcode
HAVING COUNT(*) > 1;


-- Standardization & Consistency

SELECT state, city
FROM silver.locations;


-- Checking for NULL Values

SELECT *
FROM silver.locations
WHERE zipcode IS NULL
OR city IS NULL
OR state IS NULL
OR median_income IS NULL
OR population IS NULL;

SELECT *
FROM silver.locations
WHERE zipcode = '' OR city = '' OR state = ''
OR median_income = '' OR population = '';

-- Checking for 0, NULL or Blank values in MedianIncome

SELECT zipcode, median_income
FROM silver.locations
WHERE median_income <= 0 OR median_income IS NULL OR median_income = '';
