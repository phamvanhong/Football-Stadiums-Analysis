# Football Stadiums Data Pipeline
An end-to-end data analysis project focused on building a scalable and structured data pipeline. The project involved extracting data from multiple sources on Wikipedia using Python's BeautifulSoup library, transforming it, and storing it in Azure Data Lake Gen2. The data lake follows the Medallion Architecture for efficient organization. ETL workflows were managed with Apache Airflow, while a local data warehouse was implemented with PostgreSQL (managed via pgAdmin4) for advanced analytics and reporting. Visualizations and insights were created using Power BI and PostgreSQL.

## Table of Contents
1. [Workflow](#workflow)
2. [Requirements](#requirements)
3. [Getting Started](#getting-started)
4. [Create Azure Storage Account](#create-azure-storage-account)
5. [Running the Code With Docker](#running-the-code-with-docker)
6. [Airflow Variables](#airflow-variables)
7. [End to End Processing Flow](#end-to-end-processing-flow)
8. [Data Warehouse Implementation Followed By Star Schema](#data-warehouse-implementation-followed-by-star-schema)

## Workflow
![Screenshot 2025-01-12 213900](https://github.com/user-attachments/assets/5570cf8c-74e8-410c-ac0f-a4b75d0b47bd)

## Requirements
- Python 3.12
- Apache Airflow 2.10.4
- Docker
- PostgreSQL
- Azure Portal account (If you want storage your data in the datalake)

## Getting Started
1. Clone the repository
   ```bash
   git clone https://github.com/phamvanhong/Football-Stadiums-Analysis.git
2. Install Python dependencies
   ```bash
   pip install -r requirements.txt
## Create Azure Storage Account
1. Create your Azure Storage Account
   - Using service: Azure Blod Storage or Azure Data Lake Storage Gen2
2. Create your container in Azure Storage Account
3. Medallion Data Lake Architecture

    <img src="https://github.com/user-attachments/assets/adc57f2f-fed6-42df-9e0f-e074c9c868e3" alt="azure storage account structure" width="300">

    ![Medallion DataLake Architecture](https://github.com/user-attachments/assets/0dd1d84a-e793-48a8-ac86-0946aa0ab344)
4. Medallion DataLake Architecture Explaination
   - **BRONZE layer**: The layer that only stores raw data in file-based (csv, json, parquet...)
   - **SILVER layer**: The layer that stores transformed data in file-based (csv, json, parquet...)
   - **GOLD layer**: The layer that stores fact and dimensions data (still in file-based) for implementing data warehouse and analytics.
## Running the Code with Docker
1. Building docker images in docker-compose.yml
   ```bash
   docker-compose build
2. Initialize database for airflow environtment & start airflow services
   ```bash
   docker-compose up airflow-init
   docker-compose up -d
3. If existing any change in docker-compose.yml, shuting down all docker services and restart docker services
- Shuting down all docker services
   ```bash
   docker-compose down -v
- Restart:
  ```bash
   docker-compose up -d
4. Trigger DAG on the Airflow UI
## Airflow Variables
- Setup your Azure Storage Account Secret key to Airflow Variables
   - The secret key will help the system store data (raw and transformed) into your storage account.
     ![Screenshot 2025-01-12 220319](https://github.com/user-attachments/assets/1b0c8761-fc07-4942-ade6-bc2efd227264)
## End to End Processing Flow
1. Using Python BeautifulSoup to crawl multiple data sources from Wikipedia
2. Loading raw data into BRONZE layer of Azure Data Lake Storage Gen2
3. Using Python to transform data and storing into SILVER layer
4. Trigger ETL jobs by Apache Airflow
   ![Screenshot 2025-01-17 012623](https://github.com/user-attachments/assets/4f958786-2644-4cfd-927e-c1156624835b)

5. Using Azure Data Factory to get transformed data from the SILVER layer to define facts and dimensions that will be sinked into the GOLD layer
   ![Dataflow to transform and merge data sources](https://github.com/user-attachments/assets/44f8a019-1569-4758-85a8-7be11b8be40a)
   
   ![Dataflow to define Fact Dimensions](https://github.com/user-attachments/assets/680d0c55-0c7d-451b-a849-fd564401f76d)

   ![Pipeline Management](https://github.com/user-attachments/assets/0fce9e03-44d0-404b-a00b-89ec68380018)

6. Implementing a local Data Warehouse using PostgreSQL
7. Analytics and Visualization by PostgreSQL and Power BI
## Data Warehouse Implementation Followed By Star Schema
![ERD for Schema](https://github.com/user-attachments/assets/a049c3df-c0ce-4086-8e45-4c4ede27cfe0)
