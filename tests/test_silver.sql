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

SELECT AgentID, COUNT(*) AS DuplicateCount
FROM silver.agents
GROUP BY AgentID
HAVING COUNT(*) > 1;

-- Checking for unwanted spaces
SELECT * 
FROM silver.agents
WHERE FirstName <> trim(FirstName);


-- Standardization & Consistency
-- Checking for NULL and Blank Values
SELECT Count(*)
FROM silver.agents
WHERE AgentID IS NULL
OR FirstName IS NULL
OR Surname IS NULL
OR Agency IS NULL;

SELECT *
FROM silver.agents
WHERE AgentID IS NULL
   OR FirstName IS NULL
   OR Surname IS NULL
   OR Agency IS NULL;
   
SELECT Count(*)
FROM silver.agents
WHERE FirstName = '' OR Surname = '' OR Agency = '';

-- 2) Client

-- Checking for Duplicate vales

SELECT ClientID, COUNT(*) AS DuplicateCount
FROM bronze.clients
GROUP BY ClientID
HAVING COUNT(*) > 1;


-- Checking for unwanted spaces

SELECT ClientID, Client_FirstName 
FROM silver.clients
WHERE Client_FirstName <> TRIM(Client_FirstName);
-- Standardization & Consistency
-- Checking for NULL Values

SELECT *
FROM silver.clients
WHERE ClientID IS NULL
OR Client_FirstName IS NULL
OR Client_Surname IS NULL;

SELECT COUNT(*)
FROM silver.clients
WHERE Client_FirstName =''
OR Client_Surname = '';

-- 3) Properties

-- Checking for Duplicate values

SELECT PropertyID, COUNT(*) AS DuplicateCount
FROM bronze.properties
GROUP BY PropertyID
HAVING COUNT(*) > 1;


-- Checking for unwanted spaces

SELECT PropertyID, City
FROM silver.properties
WHERE City <> TRIM(City);
-- Standardization & Consistency
-- Checking for NULL Values

SELECT *
FROM silver.properties
WHERE PropertyID IS NULL
OR City IS NULL;

SELECT *
FROM silver.properties
WHERE PropertyID =''
OR Address =''
OR City =''
OR State =''
OR ZipCode =''
OR Type =''
OR Price =''
OR SquareFeet =''
OR Bedrooms =''
OR Bathrooms =''
OR AgentID ='';

SELECT DISTINCT State 
FROM silver.properties
ORDER BY State;

-- 4) Sales

-- Checking for Duplicate vales
SELECT * FROM silver.sales;

SELECT SalesID, COUNT(*) AS DuplicateCount
FROM silver.sales
GROUP BY SalesID
HAVING COUNT(*) > 1;


-- Standardization & Consistency
-- Checking for NULL Values
SELECT *
FROM silver.sales
WHERE SalesID IS NULL
OR PropertyID IS NULL
OR ClientID IS NULL
OR SaleDate IS NULL
OR SalePrice IS NULL;

SELECT Count(*)
FROM silver.sales
WHERE SalesID = '' OR PropertyID = '' OR ClientID = ''
OR SaleDate = '' OR SalePrice = '';

-- Checking for invalid dates

SELECT SalesID, SaleDate
FROM silver.sales
WHERE 
    -- Not valid YYYY-MM-DD
    NOT (SaleDate REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
    OR SaleDate IS NULL
    OR SaleDate = '';
    
-- Checking for future dates

  SELECT SalesID, SaleDate,
       STR_TO_DATE(SaleDate, '%Y-%m-%d') AS ParsedDate
FROM silver.sales
WHERE (
        STR_TO_DATE(SaleDate, '%Y-%m-%d') > CURDATE()
     OR STR_TO_DATE(SaleDate, '%m-%d-%Y') > CURDATE()
     OR STR_TO_DATE(SaleDate, '%d/%m/%Y') > CURDATE()
);
-- Checking for 0, NULL or Blank values in Sales Price
SELECT SalesID, SalePrice
FROM silver.sales
WHERE SalePrice <= 0 OR SalePrice IS NULL OR SalePrice = '';

-- 5) Locations

SELECT * FROM silver.locations;

-- Checking for Duplicate vales

SELECT ZipCode, COUNT(*) AS DuplicateCount
FROM silver.locations
GROUP BY ZipCode
HAVING COUNT(*) > 1;


-- Standardization & Consistency

SELECT DISTINCT State FROM silver.locations;
-- Checking for NULL Values

SELECT *
FROM silver.locations
WHERE ZipCode IS NULL
OR City IS NULL
OR State IS NULL
OR MedianIncome IS NULL
OR Population IS NULL;

SELECT Count(*)
FROM silver.locations
WHERE ZipCode = '' OR City = '' OR State = ''
OR MedianIncome = '' OR Population = '';

-- Checking for 0, NULL or Blank values in MedianIncome

SELECT ZipCode, MedianIncome
FROM silver.locations
WHERE MedianIncome <= 0 OR MedianIncome IS NULL OR MedianIncome = '';
