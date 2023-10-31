-- University of Technology Sydney
-- Big Data Engineering, 2023
-- Nathan Collins
-- 12062131

{{ config(materialized='table') }}
-- Set the materialization type for the SQL model to 'table'.

WITH sorted_listings AS (
    -- Create a common table expression (CTE) called 'sorted_listings'.
    -- This CTE selects various columns from the 'facts_listings' table and performs some calculations.

    SELECT 
        prop.listing_neighbourhood, 
        -- Select the 'listing_neighbourhood' column from the 'dim_property' table.
        
        DATE_TRUNC('month', list.scraped_date) AS month_year, 
        -- Truncate the 'scraped_date' column to month granularity and rename it as 'month_year'.

        list.has_availability, -- Including the has_availability column
        -- Select the 'has_availability' column from the 'facts_listings' table.

        list.listing_id, -- Including the listing_id column
        -- Select the 'listing_id' column from the 'facts_listings' table.

        CASE 
            WHEN list.has_availability = 't' THEN list.price 
            ELSE NULL 
        END AS price,
        -- Calculate the 'price' column based on the condition of 'has_availability'.
        
        host.host_id, 
        -- Select the 'host_id' column from the 'dim_host' table.

        host.host_is_superhost,
        -- Select the 'host_is_superhost' column from the 'dim_host' table.

        review_scores_rating,
        -- Select the 'review_scores_rating' column.

        availability_30
        -- Select the 'availability_30' column.
    
    FROM public_warehouse.facts_listings list
    -- Join the 'facts_listings' table and alias it as 'list'.
    
    LEFT JOIN public_warehouse.dim_property prop ON list.surrogate_property = prop.surrogate_property 
    -- Left join with 'dim_property' based on the surrogate property keys.
    
    LEFT JOIN public_warehouse.dim_host host ON list.surrogate_host = host.surrogate_host 
    -- Left join with 'dim_host' based on the surrogate host keys.
)

SELECT 
    listing_neighbourhood,
    -- Select the 'listing_neighbourhood' column.

    month_year,
    -- Select the 'month_year' column.

    ROUND(
        (CASE 
            WHEN COUNT(DISTINCT CASE WHEN has_availability = 't' THEN listing_id END) > 0 
            THEN (COUNT(DISTINCT CASE WHEN has_availability = 't' THEN listing_id END) * 1.0 / NULLIF(COUNT(DISTINCT sorted_listings.listing_id), 0)) * 100 
            ELSE 0 
        END), 2
    ) AS active_listing_rate,
    -- Calculate the 'active_listing_rate' as a percentage of active listings.

    MIN(CASE WHEN has_availability = 't' THEN price END) AS min_price_active_listings,
    -- Calculate the minimum price of active listings.

    MAX(CASE WHEN has_availability = 't' THEN price END) AS max_price_active_listings,
    -- Calculate the maximum price of active listings.

    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price_active_listings,
    -- Calculate the median price of active listings.

    ROUND(
        AVG(CASE WHEN has_availability = 't' THEN price END), 2
    ) AS avg_price_active_listings,
    -- Calculate the average price of active listings.

    COUNT(DISTINCT host_id) AS number_of_distinct_hosts,
    -- Count the number of distinct hosts.

    ROUND(
        (CASE 
            WHEN COUNT(DISTINCT CASE WHEN host_is_superhost = '1' THEN host_id END) > 0 
            THEN (COUNT(DISTINCT CASE WHEN host_is_superhost = '1' THEN host_id END) * 1.0 / NULLIF(COUNT(DISTINCT host_id), 0)) * 100 
            ELSE 0 
        END), 2
    ) AS superhost_rate,
    -- Calculate the superhost rate as a percentage.

    ROUND(
        AVG(CASE WHEN has_availability = 't' THEN review_scores_rating END), 2
    ) AS avg_review_scores_rating_active,
    -- Calculate the average review scores for active listings.

    ROUND(
        (((COUNT(DISTINCT CASE WHEN has_availability = 't' THEN listing_id END) - LAG(COUNT(DISTINCT CASE WHEN has_availability = 't' THEN listing_id END)) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year)) * 1.0) / NULLIF(LAG(COUNT(DISTINCT CASE WHEN has_availability = 't' THEN listing_id END)) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year), 0)) * 100, 2
    ) AS active_listings_percentage_change,
    -- Calculate the percentage change in active listings.

    ROUND(
        (((COUNT(DISTINCT CASE WHEN has_availability = 'f' THEN listing_id END) - LAG(COUNT(DISTINCT CASE WHEN has_availability = 'f' THEN listing_id END)) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year)) * 1.0) / NULLIF(LAG(COUNT(DISTINCT CASE WHEN has_availability = 'f' THEN listing_id END)) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year), 0)) * 100, 2
    ) AS inactive_listings_percentage_change,
    -- Calculate the percentage change in inactive listings.

    SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) END) AS number_of_stays,
    -- Calculate the number of stays for active listings.

    ROUND(
        (CASE 
            WHEN COUNT(DISTINCT CASE WHEN has_availability = 't' THEN listing_id END) > 0 
            THEN (SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) * price END) * 1.0 / NULLIF(COUNT(DISTINCT CASE WHEN has_availability = 't' THEN listing_id END), 0))
            ELSE 0
        END), 2
    ) AS avg_estimated_revenue_per_active_listings
    -- Calculate the average estimated revenue per active listings.

FROM sorted_listings
-- Select from the 'sorted_listings' CTE.

GROUP BY listing_neighbourhood, month_year
-- Group the results by 'listing_neighbourhood' and 'month_year'.
