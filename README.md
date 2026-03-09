# Full-Stack Healthcare Data Engineering Pipeline

An end-to-end data engineering project that automates the extraction, transformation, and loading of global healthcare data and visualizes insights through an interactive analytics dashboard.

This project demonstrates a **production-style ETL pipeline** built using modern data engineering tools including **Python, Apache Airflow, Docker, MySQL, and Tableau**.

---

# Project Overview

Global healthcare datasets are often large, complex, and frequently updated.  
Analyzing them manually is inefficient and time-consuming.

This project solves that problem by building an **automated ETL pipeline** that:

1. Extracts healthcare data from WHO datasets
2. Transforms and cleans the data
3. Loads the processed data into a MySQL database
4. Visualizes insights using a Tableau dashboard

---

# Some Screenshots

**Tableau Dashboard**

<img src="https://github.com/CarlyLouis/healthcare-etl-pipeline/blob/main/Pictures/Screenshot%202026-03-08%20190759.png" width="50%"> 
 
**Airflow_1**

<img src="https://github.com/CarlyLouis/healthcare-etl-pipeline/blob/main/Pictures/Screenshot%202026-03-02%20171138.png" width="50%"> 
<br> 
 
**Airflow_2**

<img src="https://github.com/CarlyLouis/healthcare-etl-pipeline/blob/main/Pictures/Screenshot%202026-03-02%20164136.png" width="50%"> 
<br> 
 
**Running Docker Containers**

<img src="https://github.com/CarlyLouis/healthcare-etl-pipeline/blob/main/Pictures/Screenshot%202026-03-02%20164136.png" width="50%"> 
<br> 
 
**VSCode**

<img src="https://github.com/CarlyLouis/healthcare-etl-pipeline/blob/main/Pictures/Screenshot%202026-03-08%20192017.png" width="50%"> 
<br>

  
---

# Architecture

<img src="https://github.com/CarlyLouis/healthcare-etl-pipeline/blob/main/Pictures/Screenshot%202026-03-09%20172813.png">

The entire pipeline runs inside **Docker containers** to ensure consistent environments and easy deployment.

---

# Tech Stack

Python  
Apache Airflow  
Docker  
MySQL  
Tableau  

Supporting Tools

Pandas  
SQL  
Docker Compose  

---

# ETL Pipeline Workflow

The pipeline follows a classic **ETL architecture**.

### 1 Extract
Fetch healthcare indicator data from external datasets.

### 2 Transform
Clean and process the raw data.

Examples:

• Handling missing values  
• Formatting country indicators  
• Standardizing health metrics  
• Preparing data for database ingestion

### 3 Load
Load processed datasets into a **MySQL relational database**.

---

# Airflow Orchestration

The workflow is orchestrated using **Apache Airflow DAGs**.

Pipeline tasks include:

- Data extraction
- Data transformation
- Database loading

Airflow provides:

- Workflow scheduling
- Task dependencies
- Monitoring and retries
- Logging and error tracking

---

# Docker Environment

The project runs using **Docker Compose**, which spins up:

- Apache Airflow
- MySQL Database
- Supporting services

Benefits:

- Reproducible environment
- Simplified deployment
- Easy dependency management

---

# Data Visualization

Processed data feeds a **Tableau dashboard** used for analysis.

Dashboard insights include:

- Global healthcare expenditure trends
- Country comparisons
- Geographic disparities in healthcare spending
- Trend analysis across years

---

# Project Structure

<img src="https://github.com/CarlyLouis/healthcare-etl-pipeline/blob/main/Pictures/Screenshot%202026-03-09%20155851.png" width="30%">


---

# Skills Demonstrated

Data Engineering

ETL Pipeline Development

Apache Airflow Workflow Orchestration

Docker Containerization

SQL Database Design

Python Data Processing

Data Visualization

---

# Future Improvements

Possible enhancements include:

- Adding automated data quality checks
- Implementing CI/CD pipelines
- Deploying the pipeline to a cloud environment
- Creating a web dashboard for real-time insights

---

# Author

**Carly Louis**

Data Analyst | Data Engineering Enthusiast

LinkedIn  
www.linkedin.com/in/carly-louis-krlification

---
