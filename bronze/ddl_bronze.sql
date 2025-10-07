/*
=================================================================================
Create Bronze Layer Tables
==================================================================================
Script Purpose:
    This script sets up the foundational (Bronze) layer of the Data Warehouse.  
    It drops any existing tables to ensure a clean environment and then recreates 
    the raw data tables for:
        - Agents
        - Clients
        - Properties
        - Sales
        - Locations

    The Bronze layer serves as the landing zone for raw, unprocessed data 
    ingested from various source systems. These tables will later be transformed 
    and cleaned before being moved to the Silver layer.

WARNING:
    Running this script will delete any existing data in the Bronze tables 
    by dropping and recreating them.
==================================================================================
*/

USE bronze;

-- Creating data tables in the bronze layer
DROP TABLE IF EXISTS bronze.sales;
DROP TABLE IF EXISTS bronze.properties;
DROP TABLE IF EXISTS bronze.clients;
DROP TABLE IF EXISTS bronze.agents;
DROP TABLE IF EXISTS bronze.locations;

CREATE TABLE bronze.agents (
AgentID INT,
Name VARCHAR(100),
PhoneNumber VARCHAR(100),
Email VARCHAR(100),
Agency VARCHAR(100)
);


CREATE TABLE bronze.clients (
ClientID INT,
Name VARCHAR(100),
PhoneNumber VARCHAR(100),
Email VARCHAR(100)
);


CREATE TABLE bronze.properties (
    PropertyID INT,
    Address VARCHAR(255),
    City VARCHAR(100),
    State VARCHAR(50),
    ZipCode VARCHAR(10),
    Type VARCHAR(50),
    Price DECIMAL(12,2),
    SquareFeet INT,
    Bedrooms INT,
    Bathrooms INT,
    AgentID INT
);


CREATE TABLE bronze.sales (
SalesID INT,
PropertyID INT,
ClientID INT,
AgentID INT,
SaleDate VARCHAR(20),
SalePrice DECIMAL (12, 1)
);


CREATE TABLE bronze.locations(
ZipCode INT,
City VARCHAR(100),
State VARCHAR(100),
MedianIncome DECIMAL(12, 2),
Population INT
);




