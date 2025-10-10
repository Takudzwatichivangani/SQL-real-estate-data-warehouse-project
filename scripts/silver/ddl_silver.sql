/*
==============================================================================================
DDL Script: Create Silver Tables
==============================================================================================
Script Purpose:
  This script creates table in 'silver' schema, dropping existing tables
  if they already exist.
  Run this script to re-define the DDL structure of 'silver' Tables
==============================================================================================
*/

USE silver;

-- Creating data tables in the silver layer
DROP TABLE IF EXISTS silver.sales;
DROP TABLE IF EXISTS silver.properties;
DROP TABLE IF EXISTS silver.clients;
DROP TABLE IF EXISTS silver.agents;
DROP TABLE IF EXISTS silver.locations;

CREATE TABLE silver.agents (
    agent_id VARCHAR(10) PRIMARY KEY,
    agent_name VARCHAR(100),
    agent_phone_number VARCHAR(100),
    agent_email VARCHAR(100),
    agency VARCHAR(100),
    DateCreated DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE silver.clients (
    client_id VARCHAR(20) PRIMARY KEY,
    client_name VARCHAR(100),
    client_phone_number VARCHAR(100),
    client_email VARCHAR(100),
    DateCreated DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE silver.properties (
    property_id VARCHAR(10) PRIMARY KEY,
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zipcode VARCHAR(10),
    property_type VARCHAR(50),
    property_price DECIMAL(12,2),
    property_square_feet INT,
    property_bedrooms INT,
    property_bathrooms INT,
    agent_id VARCHAR(10),
    DateCreated DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (agent_id) REFERENCES silver.agents(agent_id)
);

CREATE TABLE silver.sales (
    sales_id VARCHAR(10) PRIMARY KEY,
    property_id VARCHAR(10),
    client_id VARCHAR(10),
    agent_id VARCHAR(10),
    sale_date VARCHAR(20),
    sale_price DECIMAL(12,1),
    DateCreated DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (property_id) REFERENCES silver.properties(property_id),
    FOREIGN KEY (client_id) REFERENCES silver.clients(client_id),
    FOREIGN KEY (agent_id)  REFERENCES silver.agents(agent_id)
);

CREATE TABLE silver.locations (
    zipcode INT PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(100),
    median_income DECIMAL(12,2),
    population INT,
    DateCreated DATETIME DEFAULT CURRENT_TIMESTAMP
);
