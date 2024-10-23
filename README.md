
 <div id="user-content-toc">
    <ul>
      <summary><h1 style="display: inline-block;"> Online Retail Data Pipeline </h1></summary>
    </ul>
  </div>

![Dashboard](https://github.com/user-attachments/assets/fa3ad1ab-ecc7-49ec-9391-81ed91fbeb96)
1. [ Project Overview ](#introduction)
3. [ Project Architecture ](#arch)


<a name="introduction"></a>
## 🔬 Project Overview 

This is an end to end data engineering project in which I developed an ELT pipeline to extract, analyze, and visualize insights from an online retail company's data using modern DE stack.

### 💾 Dataset

This is a transactional dataset that captures all sales between 01/12/2010 and 09/12/2011 for a UK-based, non-store online retailer. The company specializes in unique, all-occasion gifts.


The dataset link: [Dataset](https://www.kaggle.com/datasets/vijayuv/onlineretail)

### 🎯 Project Goals

- Set up your Airflow local environment with the Astro CLI.
- Create a data pipeline from scratch using Apache Airflow.
- Upload CSV files into Snowflake staging area.
- Ingest data into Snowflake database.
- Implement data quality checks in the pipeline using Soda.
- Integrate dbt and run data models with Airflow and Cosmos.
- Visualize insights using PowerBI.


<a name="arch"></a>
## 📝 Project Architecture

The end-to-end data pipeline includes the following steps:

- Downloading, processing, and uploading the initial dataset to a Data Lake *(Snowflake)*
- Moving the data from the lake to  Database
- Transforming the data in the Data Warehouse and preparing it for the dashboard *(dbt)*
- Checking the quality of the data in the Data Warehouse *(Soda)*
- Creating the dashboard *(PowerBI)*
  
You can find the detailed information on the diagram below:

![online_retail_pipeline](https://github.com/user-attachments/assets/8313de58-37c8-482d-8e4b-e166b64b4061)

:name_badge: Fun fact: i used diagrams library in python to generate this diagram [Link](https://diagrams.mingrammer.com/docs/getting-started/installation)

### 🔧 Pipeline on Airflow
![dbt_dag](https://github.com/user-attachments/assets/7ddd4f82-3660-4bd1-b77c-0e3b242e936a)




### ⚙️ Data Modeling
![Data_Modelling](https://github.com/user-attachments/assets/7f153d0d-7dc7-469b-9d09-b0b257d706d7)


### 🛠️ Technologies Used

- **Snowflake**
  - Stage
  - Data Warehouse
- **Astro SDK** for Airflow
- **Workflow orchestration:** Apache Airflow
- **Transforming data:** dbt (Data Build Tool)
- **Data quality checks:** Soda
- **Containerization:** Docker
- **Data Visualization:** PowerBI
