
-- Universtiy of Technology Sydney
-- Big Data Engineering, 2023

-- Nathan Collins
-- 12062131





-- Listings Dimension Table Configuration
-------------------------------------------------------------------#

{{
    config(
        materialized = 'table',
        unique_key = 'surrogate_property'
    )
}}



-- Select Specific Columns from Staging Data
-------------------------------------------------------------------#

SELECT

    CONCAT(CAST(listing_id AS VARCHAR), '_', CAST(scraped_date AS VARCHAR)) as surrogate_property,

    CONCAT(CAST(host_id AS VARCHAR), '_', CAST(scraped_date AS VARCHAR)) as surrogate_host,

    listing_id,
    scrape_id,
    scraped_date,
    host_id,
    price,
    has_availability,
    listing_neighbourhood,
    availability_30,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_value,
    number_of_reviews,
    review_scores_rating

FROM {{ ref('listing_stg') }}
