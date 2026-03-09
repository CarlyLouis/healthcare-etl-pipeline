FROM apache/airflow:3.1.7-python3.11

USER root

# Install system packages (MySQL client headers, compiler)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-libmysqlclient-dev gcc && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

RUN pip install apache-airflow-providers-fab
    
USER airflow

# Project-specific Python dependencies for ETL code (installed as airflow user)
RUN pip install --no-cache-dir \
    mysqlclient \
    pymysql \
    pandas \
    numpy \
    SQLAlchemy \
    requests \
    python-dotenv

