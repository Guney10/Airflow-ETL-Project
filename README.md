# Airflow-ETL-Project
An ETL pipeline built with Apache Airflow, Docker and PostgreSQL to automate COVID-19 data ingestion, transformation, and loading into a database.

## Overview
This project demonstrates an automated ETL pipeline using Apache Airflow.
It gets real-time COVID-19 data from a public API, transforms it, and loads it into a PostgreSQL database running in a Docker container.

## Tech Tools
- Workflow Orchestration: Apache Airflow

- Containerisation: Docker & Docker Compose

- Database: PostgreSQL

- Language: Python

- Source API: disease.sh - COVID-19 API

- Version Control: Git & GitHub

## Pipeline Architecture

**COVID 19 API -> Airflow DAG (Extract, Transform and Load) -> PostgreSQL Database**
