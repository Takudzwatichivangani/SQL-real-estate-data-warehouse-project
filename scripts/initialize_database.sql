/*
=================================================================================
Create Schemas
==================================================================================
Script Purpose:
    This script creates three database schemas — 'bronze', 'silver', and 'gold' — 
    to organize data into different layers of the ETL pipeline. 
    - Bronze: Stores raw and unprocessed data.
    - Silver: Contains cleaned and standardized data.
    - Gold: Holds final analytical and business-ready views.

WARNING:
    Running this script will permanently delete all objects (tables, views, etc.)
    within the 'bronze', 'silver', and 'gold' schemas. Proceed with caution.
==================================================================================
*/

-- Drop existing schemas if they exist

DROP SCHEMA IF EXISTS bronze;
DROP SCHEMA IF EXISTS silver;
DROP SCHEMA IF EXISTS gold;

-- Creating Schemas
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS gold;

