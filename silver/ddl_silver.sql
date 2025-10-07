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
    AgentID INT PRIMARY KEY,
    Agent_FirstName VARCHAR(100),
    Agent_Surname VARCHAR(100),
    Agency VARCHAR(100),
    DateCreated DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE silver.clients (
    ClientID INT PRIMARY KEY,
    Client_FirstName VARCHAR(100),
	Client_Surname VARCHAR(100),
    DateCreated DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE silver.properties (
    PropertyID INT PRIMARY KEY,
    Address VARCHAR(255),
    City VARCHAR(100),
    State VARCHAR(50),
    ZipCode VARCHAR(10),
    Type VARCHAR(50),
    Price DECIMAL(12,2),
    SquareFeet INT,
    Bedrooms INT,
    Bathrooms INT,
    AgentID INT,
    DateCreated DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (AgentID) REFERENCES silver.agents(AgentID)
);

CREATE TABLE silver.sales (
    SalesID INT PRIMARY KEY,
    PropertyID INT,
    ClientID INT,
    AgentID INT,
    SaleDate VARCHAR(20),
    SalePrice DECIMAL(12,1),
    DateCreated DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (PropertyID) REFERENCES silver.properties(PropertyID),
    FOREIGN KEY (ClientID) REFERENCES silver.clients(ClientID),
    FOREIGN KEY (AgentID)  REFERENCES silver.agents(AgentID)
);

CREATE TABLE silver.locations (
    ZipCode INT PRIMARY KEY,
    City VARCHAR(100),
    State VARCHAR(100),
    MedianIncome DECIMAL(12,2),
    Population INT,
    DateCreated DATETIME DEFAULT CURRENT_TIMESTAMP
);

