/*
==================================================================================
Silver Layer ETL Stored Procedure: silver.load_silver
==================================================================================
Script Purpose:
    This script defines a stored procedure responsible for loading cleansed and 
    standardized data from the **Bronze Layer** into the **Silver Layer** tables. 
    It performs schema alignment, data deduplication, referential integrity 
    enforcement, and value standardization across entities such as Agents, 
    Clients, Properties, Sales, and Locations.

Technical Workflow:
    1. Disables foreign key checks to allow controlled truncation and reload.
    2. Truncates existing Silver tables to ensure a clean load.
    3. Applies data cleaning and transformation logic:
        - Splits full names into first and last names.
        - Standardizes casing and formats for textual fields.
        - Handles nulls and missing values with placeholders (e.g., 'N/A').
        - Normalizes inconsistent state spellings using rule-based corrections.
        - Converts date formats to standardized SQL datetime values.
    4. Re-enables foreign key checks post-load to maintain relational consistency.

Data Engineering Context:
    This procedure represents the **Transformation and Cleansing** stage 
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
    AgentID,
    Agent_FirstName,
    Agent_Surname,
    Agency
)
SELECT 
    AgentID,
    CASE 
        WHEN CONCAT(
            UPPER(LEFT(SUBSTRING_INDEX(TRIM(Name), ' ', 1), 1)),
            LOWER(SUBSTRING(SUBSTRING_INDEX(TRIM(Name), ' ', 1), 2))
        ) = '' THEN 'N/A'
        ELSE CONCAT(
            UPPER(LEFT(SUBSTRING_INDEX(TRIM(Name), ' ', 1), 1)),
            LOWER(SUBSTRING(SUBSTRING_INDEX(TRIM(Name), ' ', 1), 2))
        )
    END AS Agent_FirstName,
    CASE
        WHEN CONCAT(
            UPPER(LEFT(SUBSTRING_INDEX(TRIM(Name), ' ', -1), 1)), 
            LOWER(SUBSTRING(SUBSTRING_INDEX(TRIM(Name), ' ', -1), 2))
        ) = '' THEN 'N/A'
        ELSE CONCAT(
            UPPER(LEFT(SUBSTRING_INDEX(TRIM(Name), ' ', -1), 1)), 
            LOWER(SUBSTRING(SUBSTRING_INDEX(TRIM(Name), ' ', -1), 2))
        )
    END AS Agent_Surname,
    CASE
        WHEN CONCAT(
            UPPER(LEFT(SUBSTRING_INDEX(REPLACE(TRIM(Agency), ',', ''), ' ', 1), 1)),
            LOWER(SUBSTRING(SUBSTRING_INDEX(REPLACE(TRIM(Agency), ',', ''), ' ', 1), 2))
        ) = '' THEN 'N/A'
        ELSE CONCAT(
            UPPER(LEFT(SUBSTRING_INDEX(REPLACE(TRIM(Agency), ',', ''), ' ', 1), 1)),
            LOWER(SUBSTRING(SUBSTRING_INDEX(REPLACE(TRIM(Agency), ',', ''), ' ', 1), 2))
        )
    END AS Agency
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY AgentID ORDER BY AgentID) AS rn
    FROM bronze.agents
) AS RankedAgents
WHERE rn = 1;

-- 2) CLIENT

TRUNCATE TABLE silver.clients;

INSERT INTO silver.clients(
ClientID,
Client_FirstName,
Client_Surname
)
SELECT ClientID,
CASE WHEN TRIM(Name) = '' THEN 'N/A'
ELSE
   CONCAT(UPPER(LEFT(SUBSTRING_INDEX(TRIM(Name), ' ', 1), 1)),
   LOWER(SUBSTRING(SUBSTRING_INDEX(TRIM(Name), ' ', 1), 2))) END AS Client_FirstName,
   
   CASE WHEN TRIM(Name) = '' THEN 'N/A'
ELSE
   CONCAT(UPPER(LEFT(SUBSTRING_INDEX(TRIM(Name), ' ', -1), 1)),
   LOWER(SUBSTRING(SUBSTRING_INDEX(TRIM(Name), ' ', -1), 2))) END AS Client_Surname
FROM bronze.clients;

-- 3) Properties

TRUNCATE TABLE silver.properties;

INSERT INTO silver.properties (
    PropertyID,
    Address,
    City,
    State,
    ZipCode,
    Type,
    Price,
    SquareFeet,
    Bedrooms,
    Bathrooms,
    AgentID
)
SELECT
    PropertyID,
    CASE WHEN Address IS NULL OR TRIM(Address) = '' THEN 'N/A'
         ELSE CONCAT(UPPER(LEFT(TRIM(Address),1)), LOWER(SUBSTRING(TRIM(Address),2))) END AS Address,
    CASE WHEN City IS NULL OR TRIM(City) = '' THEN 'Unknown'
         ELSE CONCAT(UPPER(LEFT(TRIM(City),1)), LOWER(SUBSTRING(TRIM(City),2))) END AS City,
     CASE 
        WHEN State IS NULL OR TRIM(State) = '' THEN 'Unknown'
        WHEN LOWER(TRIM(State)) IN ('aalbama','albaama') THEN 'Alabama'
        WHEN LOWER(TRIM(State)) IN ('airzona','ariozna','arizoan') THEN 'Arizona'
        WHEN LOWER(TRIM(State)) IN ('arkansas','arknansas','arknasas') THEN 'Arkansas'
        WHEN LOWER(TRIM(State)) IN ('california','cailfornia','califronia') THEN 'California'
        WHEN LOWER(TRIM(State)) IN ('colorado') THEN 'Colorado'
        WHEN LOWER(TRIM(State)) IN ('connecticut','connecitcut') THEN 'Connecticut'
        WHEN LOWER(TRIM(State)) IN ('delaware','delaawre','delawaer') THEN 'Delaware'
        WHEN LOWER(TRIM(State)) IN ('nevada','envada','neavda','nevdaa','Nevaad') THEN 'Nevada'
        WHEN LOWER(TRIM(State)) IN ('florida','flroida') THEN 'Florida'
        WHEN LOWER(TRIM(State)) IN ('georgia','goergia') THEN 'Georgia'
        WHEN LOWER(TRIM(State)) IN ('hawaii','hawiai','haawii','ahwaii') THEN 'Hawaii'
        WHEN LOWER(TRIM(State)) IN ('idaho','iadho','idaoh') THEN 'Idaho'
        WHEN LOWER(TRIM(State)) IN ('illinois','illionis','ililnois','illniois') THEN 'Illinois'
        WHEN LOWER(TRIM(State)) IN ('missouri','misosuri','imssouri','msisouri','motnana') THEN 'Missouri'
        WHEN LOWER(TRIM(State)) IN ('indiana') THEN 'Indiana'
        WHEN LOWER(TRIM(State)) IN ('iowa','ioaw','iwoa') THEN 'Iowa'
        WHEN LOWER(TRIM(State)) IN ('kansas','kanssa') THEN 'Kansas'
        WHEN LOWER(TRIM(State)) IN ('kentucky','kenutcky') THEN 'Kentucky'
        WHEN LOWER(TRIM(State)) IN ('louisiana','oluisiana','luoisiana') THEN 'Louisiana'
        WHEN LOWER(TRIM(State)) IN ('maine') THEN 'Maine'
        WHEN LOWER(TRIM(State)) IN ('maryland','maryladn','marylnad','marylnad') THEN 'Maryland'
        WHEN LOWER(TRIM(State)) IN ('massachusetts') THEN 'Massachusetts'
        WHEN LOWER(TRIM(State)) IN ('michigan','mihcigan') THEN 'Michigan'
        WHEN LOWER(TRIM(State)) IN ('minnesota') THEN 'Minnesota'
        WHEN LOWER(TRIM(State)) IN ('mississippi','missisisppi') THEN 'Mississippi'
        WHEN LOWER(TRIM(State)) IN ('montana','mnotana','motnana') THEN 'Montana'
        WHEN LOWER(TRIM(State)) IN ('new mexico','ne wmexico','new emxico','ne w mexico','New mxeico') THEN 'New Mexico'
        WHEN LOWER(TRIM(State)) IN ('new hampshire','new hamphsire','new hampshier','new hamsphire','new ahmpshire') THEN 'New Hampshire'
        WHEN LOWER(TRIM(State)) IN ('new jersey') THEN 'New Jersey'
        WHEN LOWER(TRIM(State)) IN ('new york','new yrok','enw york','nwe york') THEN 'New York'
        WHEN LOWER(TRIM(State)) IN ('north carolina') THEN 'North Carolina'
        WHEN LOWER(TRIM(State)) IN ('north dakota') THEN 'North Dakota'
        WHEN LOWER(TRIM(State)) IN ('oregon','oregno','orgeon','roegon','oergon') THEN 'Oregon'
        WHEN LOWER(TRIM(State)) IN ('ohio') THEN 'Ohio'
        WHEN LOWER(TRIM(State)) IN ('oklahoma','oklahoam','oklahoam') THEN 'Oklahoma'
        WHEN LOWER(TRIM(State)) IN ('pennsylvania','pennsyvlania','pennsyvlania') THEN 'Pennsylvania'
        WHEN LOWER(TRIM(State)) IN ('rhode island') THEN 'Rhode Island'
        WHEN LOWER(TRIM(State)) IN ('south carolina','sotuh carolina','south caroilna') THEN 'South Carolina'
        WHEN LOWER(TRIM(State)) IN ('south dakota','sotuh dakota') THEN 'South Dakota'
        WHEN LOWER(TRIM(State)) IN ('tennessee') THEN 'Tennessee'
        WHEN LOWER(TRIM(State)) IN ('texas') THEN 'Texas'
        WHEN LOWER(TRIM(State)) IN ('utah','tuah','uath') THEN 'Utah'
        WHEN LOWER(TRIM(State)) IN ('vermont') THEN 'Vermont'
        WHEN LOWER(TRIM(State)) IN ('virginia') THEN 'Virginia'
        WHEN LOWER(TRIM(State)) IN ('nberaska') THEN 'Nebraska'
        WHEN LOWER(TRIM(State)) IN ('washington') THEN 'Washington'
        WHEN LOWER(TRIM(State)) IN ('west virginia','wets virginia','wes tvirginia') THEN 'West Virginia'
        WHEN LOWER(TRIM(State)) IN ('wisconsin','wiscosnin') THEN 'Wisconsin'
        WHEN LOWER(TRIM(State)) IN ('wyoming','woyming','wyomign') THEN 'Wyoming'
        ELSE CONCAT(UPPER(LEFT(TRIM(State),1)), LOWER(SUBSTRING(TRIM(State),2)))
    END AS State,
    CASE WHEN ZipCode IS NULL OR TRIM(ZipCode) = '' THEN '00000'
         ELSE ZipCode END AS ZipCode,
    CASE WHEN Type IS NULL OR TRIM(Type) = '' THEN 'Other'
         ELSE CONCAT(UPPER(LEFT(TRIM(Type),1)), LOWER(SUBSTRING(TRIM(Type),2))) END AS Type,
    CASE WHEN Price REGEXP '^[0-9]+(\.[0-9]+)?$' THEN Price ELSE NULL END AS Price,
    CASE WHEN SquareFeet REGEXP '^[0-9]+$' THEN SquareFeet ELSE NULL END AS SquareFeet,
    CASE WHEN Bedrooms REGEXP '^[0-9]+$' THEN Bedrooms ELSE NULL END AS Bedrooms,
    CASE WHEN Bathrooms REGEXP '^[0-9]+$' THEN Bathrooms ELSE NULL END AS Bathrooms,
    CASE WHEN AgentID REGEXP '^[0-9]+$' THEN AgentID ELSE NULL END AS AgentID
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY PropertyID ORDER BY PropertyID) AS rn
    FROM bronze.properties
) AS RankedProps
WHERE rn = 1
  AND PropertyID IS NOT NULL;

-- 4) SALES

TRUNCATE TABLE silver.sales;

INSERT INTO silver.sales(
SalesID,
PropertyID,
ClientID,
AgentID,
SaleDate,
SalePrice
)
SELECT 
    SalesID,
    PropertyID,
    ClientID,
    AgentID,
    CASE
        WHEN CAST(SaleDate AS CHAR) LIKE '____-__-__' 
             THEN STR_TO_DATE(SaleDate, '%Y-%m-%d')
        WHEN CAST(SaleDate AS CHAR) LIKE '%-%-%' AND LENGTH(SaleDate) = 10 
             THEN STR_TO_DATE(SaleDate, '%m-%d-%Y')
        WHEN CAST(SaleDate AS CHAR) LIKE '%/%/%' 
             THEN STR_TO_DATE(SaleDate, '%d/%m/%Y')
        ELSE NULL
    END AS SaleDate,
    NULLIF(TRIM(SalePrice), '') AS SalePrice
FROM bronze.sales;

-- 5) Locations

TRUNCATE TABLE silver.locations;

INSERT INTO silver.locations (
    ZipCode,
    City,
    State,
    MedianIncome,
    Population
)
SELECT
    ZipCode,
    CASE 
        WHEN TRIM(City) = '' OR City IS NULL THEN 'N/A'
        ELSE CONCAT(
            UPPER(LEFT(TRIM(REPLACE(City, '  ', ' ')), 1)), 
            LOWER(SUBSTRING(TRIM(REPLACE(City, '  ', ' ')), 2))
        )
    END AS City,
    CASE 
    WHEN TRIM(State) = '' OR State IS NULL THEN 'N/A'
    WHEN LOWER(TRIM(State)) IN ('alabaam', 'alabama') THEN 'Alabama'
    WHEN LOWER(TRIM(State)) IN ('alaska', 'alsaka') THEN 'Alaska'
    WHEN LOWER(TRIM(State)) IN ('arizoan', 'arizona', 'arizona', 'arizna') THEN 'Arizona'
    WHEN LOWER(TRIM(State)) IN ('arkanass', 'arkansas', 'arkasnas') THEN 'Arkansas'
    WHEN LOWER(TRIM(State)) IN ('california') THEN 'California'
    WHEN LOWER(TRIM(State)) IN ('coloardo', 'colorado') THEN 'Colorado'
    WHEN LOWER(TRIM(State)) IN ('connecticut') THEN 'Connecticut'
    WHEN LOWER(TRIM(State)) IN ('delaware', 'dleaware', 'delawrae') THEN 'Delaware'
    WHEN LOWER(TRIM(State)) IN ('florida') THEN 'Florida'
    WHEN LOWER(TRIM(State)) IN ('georgia', 'goergia', 'gerogia', 'georiga', 'eorgia') THEN 'Georgia'
    WHEN LOWER(TRIM(State)) IN ('hawaii', 'haawii') THEN 'Hawaii'
    WHEN LOWER(TRIM(State)) IN ('idaho') THEN 'Idaho'
    WHEN LOWER(TRIM(State)) IN ('illinois', 'illinios', 'illinosi') THEN 'Illinois'
    WHEN LOWER(TRIM(State)) IN ('indiana', 'nidiana') THEN 'Indiana'
    WHEN LOWER(TRIM(State)) IN ('iowa') THEN 'Iowa'
    WHEN LOWER(TRIM(State)) IN ('kansas', 'kasnas', 'knasas') THEN 'Kansas'
    WHEN LOWER(TRIM(State)) IN ('kentucky', 'ekntucky', 'kentucyk') THEN 'Kentucky'
    WHEN LOWER(TRIM(State)) IN ('louisiana', 'louisinaa', 'loiusiana', 'lousiiana') THEN 'Louisiana'
    WHEN LOWER(TRIM(State)) IN ('maine', 'miane') THEN 'Maine'
    WHEN LOWER(TRIM(State)) IN ('maryland', 'maryladn', 'marlyand') THEN 'Maryland'
    WHEN LOWER(TRIM(State)) IN ('massachusetts') THEN 'Massachusetts'
    WHEN LOWER(TRIM(State)) IN ('michigan', 'michiagn') THEN 'Michigan'
    WHEN LOWER(TRIM(State)) IN ('minnesota') THEN 'Minnesota'
    WHEN LOWER(TRIM(State)) IN ('mississippi', 'missisisppi', 'imssissippi') THEN 'Mississippi'
    WHEN LOWER(TRIM(State)) IN ('missouri', 'imssouri') THEN 'Missouri'
    WHEN LOWER(TRIM(State)) IN ('montana', 'motnana', 'mnotana') THEN 'Montana'
    WHEN LOWER(TRIM(State)) IN ('nebraska', 'enbraska', 'nberaska', 'nebraka') THEN 'Nebraska'
    WHEN LOWER(TRIM(State)) IN ('nevada', 'nevdaa', 'neavda', 'envada') THEN 'Nevada'
    WHEN LOWER(TRIM(State)) IN ('new hampshire', 'new ahmpshire', 'new hamsphire', 'new hamphsire') THEN 'New Hampshire'
    WHEN LOWER(TRIM(State)) IN ('new jersey', 'new ejrsey') THEN 'New Jersey'
    WHEN LOWER(TRIM(State)) IN ('new mexico', 'enw mexico', 'nwe mexico') THEN 'New Mexico'
    WHEN LOWER(TRIM(State)) IN ('new york', 'nyew york') THEN 'New York'
    WHEN LOWER(TRIM(State)) IN ('north carolina', 'north carloina', 'northc arolina') THEN 'North Carolina'
    WHEN LOWER(TRIM(State)) IN ('north dakota') THEN 'North Dakota'
    WHEN LOWER(TRIM(State)) IN ('ohio') THEN 'Ohio'
    WHEN LOWER(TRIM(State)) IN ('oklahoma', 'oklahom', 'oklahoam') THEN 'Oklahoma'
    WHEN LOWER(TRIM(State)) IN ('oregon', 'orgeon', 'oregno', 'oergon', 'roegon') THEN 'Oregon'
    WHEN LOWER(TRIM(State)) IN ('pennsylvania', 'pennsyvlania') THEN 'Pennsylvania'
    WHEN LOWER(TRIM(State)) IN ('rhode island', 'rhdoe island', 'rhodei sland', 'rhodoe island') THEN 'Rhode Island'
    WHEN LOWER(TRIM(State)) IN ('south carolina', 'south carloina', 'sotuh carolina') THEN 'South Carolina'
    WHEN LOWER(TRIM(State)) IN ('south dakota', 'sotuh dakota') THEN 'South Dakota'
    WHEN LOWER(TRIM(State)) IN ('tennessee', 'etnnessee', 'tennesese') THEN 'Tennessee'
    WHEN LOWER(TRIM(State)) IN ('texas', 'txeas') THEN 'Texas'
    WHEN LOWER(TRIM(State)) IN ('utah', 'tuah') THEN 'Utah'
    WHEN LOWER(TRIM(State)) IN ('vermont') THEN 'Vermont'
    WHEN LOWER(TRIM(State)) IN ('virginia', 'virginai', 'ivrginia') THEN 'Virginia'
    WHEN LOWER(TRIM(State)) IN ('washington') THEN 'Washington'
    WHEN LOWER(TRIM(State)) IN ('west virginia', 'wes tvirginia', 'wset virginia') THEN 'West Virginia'
    WHEN LOWER(TRIM(State)) IN ('wisconsin', 'wiscosnin') THEN 'Wisconsin'
    WHEN LOWER(TRIM(State)) IN ('wyoming', 'woyming', 'ywoming', 'wyomign') THEN 'Wyoming'
    ELSE CONCAT(UPPER(LEFT(TRIM(State), 1)), LOWER(SUBSTRING(TRIM(State), 2)))
END AS State,
	MedianIncome,
    Population
FROM bronze.locations;

SET FOREIGN_KEY_CHECKS = 1;
END$$
DELIMITER ;
