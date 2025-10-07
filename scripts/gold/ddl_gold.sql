
/*
==================================================================================
Gold Layer Unified Star Schema View
==================================================================================
Script Purpose:
    This script creates a consolidated analytical view in the Gold Layer 
    that merges property, sales, agent, client, and location data into a 
    unified star schema. The resulting dataset provides a complete 360° view 
    of real estate transactions for downstream reporting, dashboarding, and 
    advanced analytics use cases.

Data Engineering Context:
    This view represents the **final curated analytical model** within the 
    Medallion Architecture (Bronze → Silver → Gold). It transforms normalized 
    Silver layer structures into a denormalized, query-optimized schema 
    tailored for BI tools, trend analysis, and predictive modeling.

==================================================================================
*/

CREATE OR REPLACE VIEW gold.dim_properties AS
SELECT 
    p.PropertyID,
    p.Address,
    p.City,
    p.State,
    p.ZipCode,
    p.Type,
    p.Price,
    p.SquareFeet,
    p.Bedrooms,
    p.Bathrooms,
    COALESCE(l.MedianIncome, 'N/A') AS MedianIncome,
    COALESCE(l.Population, 'N/A') AS Population,
    COALESCE(a.Agency, 'N/A') AS Agency
FROM silver.properties p
LEFT JOIN silver.locations l
       ON p.ZipCode = l.ZipCode
LEFT JOIN silver.agents a
       ON p.AgentID = a.AgentID;

CREATE OR REPLACE VIEW gold.fact_sales_agents_clients AS
SELECT
    s.SalesID,
    s.PropertyID,
    s.ClientID,
    s.AgentID,
    s.SaleDate,
    s.SalePrice,
    
    CONCAT(
        COALESCE(c.Client_FirstName, 'N/A'), ' ',
        COALESCE(c.Client_Surname, '')
    ) AS ClientName,
   CONCAT(
        COALESCE(a.Agent_FirstName, 'N/A'), ' ', 
        COALESCE(a.Agent_Surname, '')
    ) AS AgentName,
    a.Agency AS Agency
    
FROM silver.sales s
LEFT JOIN silver.agents a
       ON s.AgentID = a.AgentID
LEFT JOIN silver.clients c
       ON s.ClientID = c.ClientID;



