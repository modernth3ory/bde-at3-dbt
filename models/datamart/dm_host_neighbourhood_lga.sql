
-- Universtiy of Technology Sydney
-- Big Data Engineering, 2023
-- Nathan Collins
-- 12062131





-- Creating a CTE to transform host data and add neighborhood's LGA information.
----------------------------------------------------------------------
WITH host_neighbourhood_lga_transform AS (
    SELECT 
        host.*,
        suburb.lga_name AS host_neighbourhood_lga,
        TO_CHAR(host.scraped_date, 'MM/YYYY') AS month_year
    FROM 
        public_warehouse.dim_host host
    LEFT JOIN 
        public_warehouse.dim_suburb suburb ON host.host_neighbourhood = suburb.suburb_name
),

-- Calculating the count of distinct hosts in each neighborhood's LGA for each month-year.
----------------------------------------------------------------------
distinct_host_count AS (
    SELECT
        TO_CHAR(h.scraped_date, 'MM/YYYY') AS month_year,
        h.host_neighbourhood_lga,
        COUNT(DISTINCT h.host_id) AS num_distinct_hosts
    FROM
        host_neighbourhood_lga_transform h
    GROUP BY
        TO_CHAR(h.scraped_date, 'MM/YYYY'),
        h.host_neighbourhood_lga
),

-- Calculating the estimated revenue for hosts in each neighborhood's LGA for each month-year.
----------------------------------------------------------------------
estimated_revenue AS (
    SELECT
        TO_CHAR(l.scraped_date, 'MM/YYYY') AS month_year,
        h.host_neighbourhood_lga,
        SUM((30 - l.availability_30) * l.price) AS total_estimated_revenue
    FROM
        public_warehouse.facts_listings l
    LEFT JOIN
        host_neighbourhood_lga_transform h ON l.surrogate_host = h.surrogate_host
    WHERE
        l.has_availability = 't'
    GROUP BY
        TO_CHAR(l.scraped_date, 'MM/YYYY'),
        h.host_neighbourhood_lga
)

-- Select and calculate final statistics for the report.
----------------------------------------------------------------------
SELECT
    dhct.month_year,
    dhct.host_neighbourhood_lga,
    dhct.num_distinct_hosts,
    er.total_estimated_revenue,
    CASE 
        WHEN dhct.num_distinct_hosts > 0 THEN ROUND(er.total_estimated_revenue / dhct.num_distinct_hosts, 2)
        ELSE 0
    END AS estimated_revenue_per_host
FROM
    distinct_host_count dhct
LEFT JOIN
    estimated_revenue er ON dhct.month_year = er.month_year AND dhct.host_neighbourhood_lga = er.host_neighbourhood_lga
ORDER BY
    dhct.month_year, dhct.host_neighbourhood_lga
