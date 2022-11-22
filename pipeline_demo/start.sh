#!/bin/bash

#echo -e "AIRFLOW_UID=$(id -u)" > .env

# Build Docker images
docker build -f dags/ppl_immo/model/Dockerfile -t airflow_model:latest dags/ppl_immo/model;
docker build -f dags/ppl_immo/scraper/Dockerfile -t airflow_scraper:latest dags/ppl_immo/scraper;
docker build -f dags/ppl_immo/sql/Dockerfile -t airflow_sql:latest dags/ppl_immo/sql;
docker build -f dags/ppl_immo/visualization/Dockerfile -t airflow_visualization:latest dags/ppl_immo/visualization;

# Run Airflow
docker compose up airflow-init;
docker compose up;