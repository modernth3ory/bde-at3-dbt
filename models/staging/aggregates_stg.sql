
-- Universtiy of Technology Sydney
-- Big Data Engineering, 2023

-- Nathan Collins
-- 12062131










-- Prior to staging, SQL queries were employed in Postgres to determine if ranges were appropriate to meet the criteria.

----------------------------------------------------------------------------------------------------------------------------#

-- Records were cross-checked with existing statistics, to account for variance between year of census. 
-- ( https://www.abs.gov.au/statistics/people/population/national-state-and-territory-population/latest-release )

-- No records appeared wildly exaggerated or improbable, and thus were deemed appropriate for staging.







-------------------------------------------------------------------#
-- Staging | This model is used to stage the aggregate data from the raw schema.
-------------------------------------------------------------------#


-- Step 0: Define the staging model for the aggregate data.
-------------------------------------------------------------------#

{{
    config(
        materialized='view'
    )
}}



-- Step 1: Omitting duplicates.
-------------------------------------------------------------------#

SELECT *
FROM raw.aggregates
