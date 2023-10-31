
-- University of Technology Sydney
-- Big Data Engineering, 2023
-- Nathan Collins
-- 12062131






-- Datamart dm_property_type

-- Set configuration for materialized view.
{{ config(materialized='view') }}

-- Create a Common Table Expression (CTE) called sorted_listings.
WITH sorted_listings AS (
    -- Select relevant columns and calculate derived values.
    SELECT 
        prop.property_type,
        prop.room_type,
        prop.accommodates,
        DATE_TRUNC('month', list.scraped_date) AS month_year,
        list.has_availability, 
        list.listing_id, 
        CASE 
            WHEN list.has_availability = 't' THEN list.price 
            ELSE NULL 
        END AS price,
        host.host_id,
        host.host_is_superhost,
        review_scores_rating,
        availability_30
    FROM public_warehouse.facts_listings list
    LEFT JOIN public_warehouse.dim_property prop ON list.surrogate_property = prop.surrogate_property 
    LEFT JOIN public_warehouse.dim_host host ON list.surrogate_host = host.surrogate_host
)

-- Select and aggregate statistics for the dm_property_type datamart.
SELECT 
    property_type,
    room_type,
    accommodates,
    month_year,

    -- Calculate the active listing rate.
    ROUND(
        (CASE 
            WHEN COUNT(DISTINCT CASE WHEN has_availability = 't' THEN listing_id END) > 0 THEN (COUNT(DISTINCT CASE WHEN has_availability = 't' THEN listing_id END) * 1.0 / NULLIF(COUNT(DISTINCT sorted_listings.listing_id), 0)) * 100 
            ELSE 0 
        END), 2
    ) AS active_listing_rate,

    -- Calculate the minimum price of active listings.
    MIN(CASE WHEN has_availability = 't' THEN price END) AS min_price_active_listings,

    -- Calculate the maximum price of active listings.
    MAX(CASE WHEN has_availability = 't' THEN price END) AS max_price_active_listings,

    -- Calculate the median price of active listings.
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price_active_listings,

    -- Calculate the average price of active listings.
    ROUND(
        AVG(CASE WHEN has_availability = 't' THEN price END), 2
    ) AS avg_price_active_listings,

    -- Calculate the number of distinct hosts.
    COUNT(DISTINCT host_id) AS number_of_distinct_hosts,

    -- Calculate the superhost rate.
    ROUND(
        (CASE 
            WHEN COUNT(DISTINCT CASE WHEN host_is_superhost = '1' THEN host_id END) > 0 THEN (COUNT(DISTINCT CASE WHEN host_is_superhost = '1' THEN host_id END) * 1.0 / NULLIF(COUNT(DISTINCT host_id), 0)) * 100 
            ELSE 0 
        END), 2
    ) AS superhost_rate,

    -- Calculate the average review scores rating for active listings.
    ROUND(
        AVG(CASE WHEN has_availability = 't' THEN review_scores_rating END), 2
    ) AS avg_review_scores_rating_active,

    -- Calculate the percentage change in active listings.
    ROUND(
        (((COUNT(DISTINCT CASE WHEN has_availability = 't' THEN listing_id END) - LAG(COUNT(DISTINCT CASE WHEN has_availability = 't' THEN listing_id END)) OVER (PARTITION BY property_type, room_type, accommodates, month_year ORDER BY month_year)) * 1.0) / NULLIF(LAG(COUNT(DISTINCT CASE WHEN has_availability = 't' THEN listing_id END)) OVER (PARTITION BY property_type, room_type, accommodates, month_year ORDER BY month_year), 0)) * 100, 2
    ) AS active_listings_percentage_change,

    -- Calculate the percentage change in inactive listings.
    ROUND(
        (((COUNT(DISTINCT CASE WHEN has_availability = 'f' THEN listing_id END) - LAG(COUNT(DISTINCT CASE WHEN has_availability = 'f' THEN listing_id END)) OVER (PARTITION BY property_type, room_type, accommodates, month_year ORDER BY month_year)) * 1.0) / NULLIF(LAG(COUNT(DISTINCT CASE WHEN has_availability = 'f' THEN listing_id END)) OVER (PARTITION BY property_type, room_type, accommodates, month_year ORDER BY month_year), 0)) * 100, 2
    ) AS inactive_listings_percentage_change,

    -- Calculate the number of stays.
    SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) END) AS number_of_stays,

    -- Calculate the average estimated revenue per active listings.
    ROUND(
        (CASE 
            WHEN COUNT(DISTINCT CASE WHEN has_availability = 't' THEN listing_id END) > 0 THEN (SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) * price END) * 1.0 / NULLIF(COUNT(DISTINCT CASE WHEN has_availability = 't' THEN listing_id END), 0))
            ELSE 0
        END), 2
    ) AS avg_estimated_revenue_per_active_listings
FROM sorted_listings
GROUP BY property_type, room_type, accommodates, month_year
