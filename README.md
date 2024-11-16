# CDE-Capstone-Project


## Background Story

A travel Agency reached out to CDE, their business model involves recommending tourist location to their customers based on different data points, they want one of our graduates to build a Data Platform that will process the data from the Country rest API [HERE](https://restcountries.com/v3.1/all) into their cloud based Database/Data Warehouse for predictive analytics by their Data Science team.

## Overview

This project implements a data platform for a travel agency to process and analyze country data for tourist location recommendations. The platform extracts data from the Country REST API, stores it in a cloud-based data lake, and transforms it for predictive analytics.

## Architecture

## Key Components

- Data Ingestion: REST API data extraction

- Storage Layers:

    - Raw Layer: Cloud Object Storage (JSON format)
    - Processed Layer: Cloud Object Storage (Parquet format)

- Orchestration: Apache Airflow
- Data Transformation: dbt
- Infrastructure: Terraform
- CI/CD: GitHub Actions


## Features

- Full API data extraction and storage in Parquet format
- Processed data attributes include:

    - Country information (name, official name, native name)
    - Geographic details (capital, region, subregion, area)
    - Demographic data (population, languages)
    - Economic indicators (currency details)
    - Administrative data (UN membership, independence status)

- Automated workflow orchestration
- Infrastructure as Code (IaC)
- Automated testing and deployment
- Data modeling (Fact and Dimension tables)

