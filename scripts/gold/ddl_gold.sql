/*
==================================================================================
Gold Layer Unified Star Schema View
==================================================================================
Script Purpose:
    This script creates a consolidated analytical view in the Gold Layer 
    that consists of 3 Views namely, dim_property, fact_sales and dim_client. 
    The resulting dataset provides a complete 360° view 
    of real estate transactions for downstream reporting, dashboarding, and 
    advanced analytics use cases.

Data Engineering Context:
    This view represents the **final curated analytical model** within the 
    Medallion Architecture (Bronze → Silver → Gold). It transforms normalized 
    Silver layer structures into a denormalized, query-optimized schema 
    tailored for BI tools, trend analysis, and predictive modeling.

==================================================================================
*/

-- 1) gold.dim_properties

CREATE OR REPLACE VIEW gold.dim_properties AS
SELECT 
    p.property_id,
    p.address,
    p.city,
    p.state,
    p.zipcode,
    p.property_type,
    p.property_price,
    p.property_square_feet,
    p.property_bedrooms,
    p.property_bathrooms,
    COALESCE(l.median_income, 'N/A') AS median_income,
    COALESCE(l.population, 'N/A') AS population,
    COALESCE(a.agent_name, 'N/A') AS agent_name,
    COALESCE(a.agent_phone_number, 'N/A') AS agent_phone_number,
    COALESCE(a.agent_email, 'N/A') AS agent_email,
    COALESCE(a.agency, 'N/A') AS agency
FROM silver.properties p
LEFT JOIN silver.locations l
       ON p.zipcode = l.zipcode
LEFT JOIN silver.agents a
       ON p.agent_id = a.agent_id;


-- 2) gold.fact_sales

CREATE OR REPLACE VIEW gold.fact_sales AS
SELECT
    s.sales_id,
    s.property_id,
    s.client_id,
    s.agent_id,
    COALESCE(c.client_name, 'N/A') AS client_name,
    COALESCE(a.agency, 'N/A') AS agency,
    s.sale_date,
    s.sale_price

FROM silver.sales s
LEFT JOIN silver.agents a
       ON s.agent_id = a.agent_id
LEFT JOIN silver.clients c
       ON s.client_id = c.client_id;

-- 3) gold.dim_clients

CREATE OR REPLACE VIEW gold.dim_clients AS
SELECT 
    c.client_id,
    c.client_name,
    c.client_phone_number,
    c.client_email
FROM silver.clients c;

