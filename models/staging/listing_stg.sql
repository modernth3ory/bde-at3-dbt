
-- Universtiy of Technology Sydney
-- Big Data Engineering, 2023

-- Nathan Collins
-- 12062131









-- Prior to staging, SQL queries were employed in Postgres to determine if ranges were appropriate to meet the criteria.
---------------------------------------------------------------------------------------------------------------------------#

-- Records were cross-checked with existing statistics, to account for variance between year of census. 
-- ( https://www.abs.gov.au/statistics/people/population/national-state-and-territory-population/latest-release )

-- No records appeared wildly exaggerated or improbable, and thus were deemed appropriate for staging.

















-------------------------------------------------------------------#
-- Staging | This model is used to stage the listing data from the raw schema.
-------------------------------------------------------------------#





-- Step 0: Define the staging model for the listing data.
-------------------------------------------------------------------#
{{
    config(
        materialized='view'
    )
}}



-- Step 1: Omit duplicates.
-------------------------------------------------------------------#

WITH deduplicated_listings AS (
    SELECT DISTINCT *
    FROM raw.listings
),




-- Step 2: Handle invalid numeric values and flexible date formats.
-------------------------------------------------------------------#

listings_filtered_invalid_numerics AS (
    SELECT 
        listing_id, 
        scrape_id, 
        CASE
            WHEN scraped_date ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(scraped_date, 'DD/MM/YYYY')
            ELSE TO_DATE(scraped_date, 'YYYY-MM-DD') 
        END AS scraped_date,
        host_id, 
        host_name, 
        CASE
            WHEN host_since ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(host_since, 'DD/MM/YYYY')
            ELSE NULL
        END AS host_since,
        host_is_superhost, 
        host_neighbourhood, 
        listing_neighbourhood, 
        property_type, 
        room_type, 
        accommodates, 
        price, 
        has_availability, 
        availability_30, 
        number_of_reviews, 
        CASE 
            WHEN review_scores_rating BETWEEN 0 AND 100 THEN review_scores_rating 
            ELSE NULL 
        END AS review_scores_rating, 
        CASE 
            WHEN review_scores_accuracy BETWEEN 0 AND 100 THEN review_scores_accuracy 
            ELSE NULL 
        END AS review_scores_accuracy, 
        CASE 
            WHEN review_scores_cleanliness BETWEEN 0 AND 100 THEN review_scores_cleanliness 
            ELSE NULL 
        END AS review_scores_cleanliness, 
        CASE 
            WHEN review_scores_checkin BETWEEN 0 AND 100 THEN review_scores_checkin 
            ELSE NULL 
        END AS review_scores_checkin, 
        CASE 
            WHEN review_scores_communication BETWEEN 0 AND 100 THEN review_scores_communication 
            ELSE NULL 
        END AS review_scores_communication, 
        CASE 
            WHEN review_scores_value BETWEEN 0 AND 100 THEN review_scores_value 
            ELSE NULL 
        END AS review_scores_value
    FROM deduplicated_listings
)



-- Step 3: Select the final result.
-------------------------------------------------------------------#

SELECT *
FROM listings_filtered_invalid_numerics
