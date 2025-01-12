# Football-Stadiums-Analysis
This is a Football Stadiums Analysis end-to-end project starting from crawling multiple data sources from Wikipedia using BeautifulSoup of the Python library, transforming, and pushing data to Azure Data Lake, which is structured to follow the Medallion Architecture. ETL jobs are managed by Apache Airflow. Implementing a data warehouse locally with PostgreSQL (using pgAdmin4) and using Power BI for visualization and analytics.

## Table of Contents
1. [System Architecture](#system-architecture)
2. [Requirements](#requirements)
3. [Getting Started](#getting-started)
4. [Create Azure Storage Account](#create-azure-storage-account)
5. [Running the Code With Docker](#running-the-code-with-docker)
6. [How It Works](#how-it-works)
7. [Video](#video)

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

