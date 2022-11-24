#!/bin/bash

# Build Docker images
docker build -f dags/scraping/Dockerfile -t airflow_scraper:latest ./dags/scraper;
docker build -f dags/cleaning/Dockerfile -t airflow_cleaning:latest ./dags/data_cleaning;
docker build -f dags/model_training/Dockerfile -t airflow_cleaning:latest ./dags/model_training;
docker build -f dags/dashboard/Dockerfile -t airflow_cleaning:latest ./dags/dashboard;

# Init Airflow
docker compose up airflow-init;
# Run Airflow
docker compose up -d;