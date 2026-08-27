-- Query to identify which solar systems (sites) are actively transmitting data
-- This query analyzes recent data activity to determine which sites are "good" (actively reporting)

WITH 
-- Get the most recent record for each site
recent_sites AS (
    SELECT DISTINCT ON (site_id) 
        site_id,
        name,
        state,
        install_date,
        turn_on_date,
        valid_data_date,
        record_date
    FROM sites 
    ORDER BY site_id, record_date DESC
),

-- Get the most recent hardware configuration for each site
recent_hardware AS (
    SELECT DISTINCT ON (hardware_id) 
        hardware_id,
        site_id,
        function_code,
        hardware_name,
        hardware_type,
        installation_date,
        flags
    FROM hardware 
    ORDER BY hardware_id, record_date DESC
),

-- Count recent data points for each site-metric combination (last 30 days)
recent_activity AS (
    SELECT 
        h.site_id,
        hm.metric,
        COUNT(*) as data_points,
        MIN(hm.ts) as earliest_data,
        MAX(hm.ts) as latest_data
    FROM hardware_metrics hm
    JOIN recent_hardware h ON h.hardware_id = ANY(hm.hardware_ids)
    WHERE hm.ts >= NOW() - INTERVAL '30 days'
    GROUP BY h.site_id, hm.metric
),

-- Aggregate activity by site
site_activity_summary AS (
    SELECT 
        rs.site_id,
        rs.name,
        rs.state,
        rs.install_date,
        rs.turn_on_date,
        rs.valid_data_date,
        COUNT(DISTINCT ra.metric) as active_metrics,
        SUM(ra.data_points) as total_data_points,
        MAX(ra.latest_data) as most_recent_data,
        MIN(ra.earliest_data) as earliest_recent_data
    FROM recent_sites rs
    LEFT JOIN recent_activity ra ON rs.site_id = ra.site_id
    GROUP BY rs.site_id, rs.name, rs.state, rs.install_date, rs.turn_on_date, rs.valid_data_date
),

-- Define what makes a site "good" (actively transmitting)
good_sites AS (
    SELECT 
        *,
        CASE 
            WHEN active_metrics >= 3 AND total_data_points >= 100 THEN 'Excellent'
            WHEN active_metrics >= 2 AND total_data_points >= 50 THEN 'Good'
            WHEN active_metrics >= 1 AND total_data_points >= 20 THEN 'Fair'
            ELSE 'Poor'
        END as transmission_quality,
        CASE 
            WHEN most_recent_data >= NOW() - INTERVAL '7 days' THEN 'Active'
            WHEN most_recent_data >= NOW() - INTERVAL '30 days' THEN 'Recent'
            WHEN most_recent_data >= NOW() - INTERVAL '90 days' THEN 'Stale'
            ELSE 'Inactive'
        END as recency_status
    FROM site_activity_summary
),

-- Get hardware details for good sites
good_sites_hardware AS (
    SELECT 
        gs.*,
        COUNT(DISTINCT rh.hardware_id) as total_hardware,
        STRING_AGG(DISTINCT rh.hardware_type, ', ') as hardware_types
    FROM good_sites gs
    LEFT JOIN recent_hardware rh ON gs.site_id = rh.site_id
    WHERE gs.transmission_quality IN ('Excellent', 'Good', 'Fair')
    GROUP BY gs.site_id, gs.name, gs.state, gs.install_date, gs.turn_on_date, 
             gs.valid_data_date, gs.active_metrics, gs.total_data_points, 
             gs.most_recent_data, gs.earliest_recent_data, gs.transmission_quality, gs.recency_status
)

-- Final output: Good solar systems with their transmission status
SELECT 
    site_id,
    name,
    state,
    install_date,
    turn_on_date,
    valid_data_date,
    active_metrics,
    total_data_points,
    most_recent_data,
    earliest_recent_data,
    transmission_quality,
    recency_status,
    total_hardware,
    hardware_types
FROM good_sites_hardware
ORDER BY 
    CASE transmission_quality
        WHEN 'Excellent' THEN 1
        WHEN 'Good' THEN 2
        WHEN 'Fair' THEN 3
        ELSE 4
    END,
    total_data_points DESC;


