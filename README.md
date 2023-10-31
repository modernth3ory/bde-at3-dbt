
To use, type **dbt run** into the terminal command prompt.

To understand the setup procedure, see the report provided in the **extra** folder.




Big Data Engineering Project 3
____________________________________

Objective:
______________

Completed a Big Data engineering project focused on constructing production-level ELT data pipelines using Airflow. The aim was to process, clean, and load datasets into a data warehouse and data mart for analytical purposes.


Datasets Employed:
______________

Airbnb: Information from May 2020 to April 2021 centered on Sydney, which details rental patterns, price variations, host-guest interactions, and more. Original data regularly purged from its source.
Census: A comprehensive dataset from the Australian Bureau of Statistics reflecting Australia's population and housing characteristics as of 2016.
Implementation:

Environment Setup: 
______________

Established an environment utilizing Airflow, Postgres (on GCP with Cloud Composer and SQL instance), and dbt hosted on a private Github repo.
Data Integration: Loaded 12 months of Airbnb listing data for Sydney and specific tables from the 2016 Census into the Postgres database using Airflow.
Data Warehouse Design: Using dbt, architected a four-layered data warehouse (raw, staging, warehouse, datamart) on Postgres with specific naming conventions and materialization requirements.
Analytical Views: In the datamart layer, three views were created:
dm_listing_neighbourhood: Data per listing neighbourhood and month/year.
dm_property_type: Data per property type, room type, accommodates, and month/year.
dm_host_neighbourhood: Data per host neighbourhood converted to LGA and month/year.
Ad-hoc Analysis: Addressed key business questions through SQL queries on Postgres to uncover insights related to population differences in listing neighbourhoods, optimal listing types, host preferences, and revenue evaluations against median mortgage repayments.


Deliverables:
______________

- SQL queries for data loading and analysis.
- Airflow DAG script.
- dbt project repository on GitHub.
- A comprehensive "handover" report detailing the project overview, step-by-step explanations, challenges encountered, solutions employed, and evidence-backed answers to business questions.


Assessment Criteria:
______________

Project quality was assessed based on the quality of code, justification for data-related decisions, quality of findings, and the clarity and professionalism of the written report.


Timeline:
______________

Project completed before the stipulated deadline of 31st October, ensuring adherence to submission guidelines and benefiting from early submission bonuses.