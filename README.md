# 🏗️ SQL Real Estate Data Warehouse Project  

## 📖 Project Overview  
This project demonstrates the design and implementation of a **data warehouse for real estate analytics** using SQL and the **Bronze–Silver–Gold architecture**.  
The objective is to establish a scalable pipeline that transforms raw property, sales, client, and agent data into **clean, relational, and analysis-ready datasets**.  

It provides a solid foundation for **reporting, business intelligence, and performance analytics** in the real estate domain.  

---

## 🧱 Architecture  
The data warehouse follows a **layered ELT architecture**, designed to progressively refine data:  

| Layer | Description |
|--------|-------------|
| **Bronze** | Raw data ingestion layer — captures data as-is from source systems. |
| **Silver** | Cleansed and standardized layer — applies data quality rules, normalization, and relational integrity. |
| **Gold** | Analytical layer — aggregates and models data into **fact** and **dimension** views optimized for insights. |

---

## ⚙️ Project Objectives  
- Establish a **clean database architecture** for real estate data.  
- Design and implement **SQL-based ETL transformations**.  
- Build **fact and dimension views** for analytical reporting.  
- Demonstrate **data modeling, referential integrity, and standardization** techniques.  

---

## 🗂️ Database Design  

### Schemas
- **bronze** – stores raw data tables.  
- **silver** – stores cleaned, relational data with foreign key constraints.  
- **gold** – contains analytical views (fact and dimension tables).  

### Core Entities
- **Agents** – agent details and agencies.  
- **Clients** – client information.  
- **Properties** – property attributes and prices.  
- **Sales** – property transactions.  
- **Locations** – regional demographic data.

---

## 📊 Analytical Views (Gold Layer)  

### `gold.dim_properties`  
Provides enriched property information by joining property, agent, and location data.  
Includes attributes such as address, price, property type, median income, and population.  

### `gold.fact_sales_agents_clients`  
Combines sales data with agent and client information for performance analysis.  
Useful for understanding sales trends, agent activity, and client behavior.  

---

## 🧠 Key Learnings  
- Data warehouse design using **SQL and dimensional modeling**.  
- Implementation of **Bronze–Silver–Gold data architecture**.  
- Application of **COALESCE, JOINs, foreign keys, and view creation** for ETL.  
- Reinforcement of **data governance principles** through schema separation and constraints.  

---

## 🧰 Tech Stack  
- **Database:** MySQL  
- **Language:** SQL  
- **Tools:** MySQL Workbench / DBeaver  
- **Architecture:** Bronze → Silver → Gold  

---
   ```bash
   git clone https://github.com/<your-username>/sql-real-estate-data-warehouse.git
