# Medical Data Lakehouse Project

This project is a small local data lakehouse built from an OpenNeuro dataset to show my data engineering skills on medical research data.

I used the OpenNeuro dataset **ds000030 (version 1.0.0)** and built a simple **Bronze / Silver / Gold** workflow for organizing raw research data into structured tables.

## What this project does

- reads raw BIDS-style research files and metadata
- parses file structure into a structured table
- cleans participant and phenotype data
- creates Delta tables for organized storage
- builds summary tables for easier analysis

## Why I built it

Medical research data is often stored in nested folders with many TSV and JSON files. This makes it hard to inspect and use directly.

I built this project to practice turning raw research data into cleaner, more useful tables for downstream research workflows.

## Dataset

- Source: **OpenNeuro ds000030**
- Version: **1.0.0**

For this portfolio project:

- I used a subset of subjects to build the transformed tables
- the repository is a lightweight demo version
- the focus is on pipeline design and data organization

## Lakehouse layers

### Bronze
Raw data and metadata kept close to the original source.

### Silver
Cleaned and structured tables, including:

- BIDS file inventory
- participants table
- phenotype table
- phenotype dictionary table

### Gold
Simple summary tables for quick inspection and analysis.

## Tools used

- Python
- Pandas
- PySpark
- Delta Lake

## What this project shows

This project shows that I can:

- work with real medical research data
- organize messy metadata
- build ETL-style data pipelines
- turn raw files into structured tables
- apply data engineering ideas to research datasets
