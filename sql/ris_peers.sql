WITH prefix_counts AS (
    SELECT 
        peer_ip, 
        peer_as, 
        CASE 
            WHEN prefix LIKE '%:%' THEN 6 -- IPv6 addresses contain colons
            ELSE 4 -- IPv4 addresses do not contain colons
        END AS prefix_ipversion, 
        host,
        -- Set dump to the current timestamp when the query is run, ensuring it is timezone-aware
        CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS dump,
        COUNT(prefix) AS prefix_count
    FROM ris
    -- No filter on timestamp, so it processes all data from the ris table
    GROUP BY peer_ip, peer_as, prefix_ipversion, host
),
filtered_peers AS (
    SELECT *
    FROM prefix_counts
    WHERE prefix_count > CASE 
                            WHEN prefix_ipversion = 4 THEN 100000
                            ELSE 1000
                        END
),
ranked_counts AS (
    SELECT 
        peer_ip, 
        peer_as, 
        prefix_ipversion, 
        host,
        prefix_count,
        dump,  -- Include the current timestamp as the dump
        ROW_NUMBER() OVER (PARTITION BY dump, prefix_ipversion ORDER BY prefix_count) AS row_num,
        COUNT(*) OVER (PARTITION BY dump, prefix_ipversion) AS total_count
    FROM filtered_peers
),
prefix_medians AS (
    -- Calculate median based on the entire dataset in this query run
    SELECT 
        dump,
        prefix_ipversion, 
        AVG(prefix_count) AS median_prefix_count
    FROM ranked_counts
    WHERE row_num IN (total_count / 2, total_count / 2 + 1)
    GROUP BY dump, prefix_ipversion
)
SELECT 
    pc.peer_ip, 
    pc.peer_as, 
    pc.prefix_ipversion, 
    pc.host,
    pc.prefix_count,
    pc.dump,
    CASE 
        -- Automatically tag older IPv6 peers as full if their dump timestamp is before a certain date
        WHEN pc.prefix_ipversion = 6 AND pc.dump < '2006-01-01 00:00:00' THEN 'F'
        -- Tag peers as full if their prefix count is greater than 90% of the median
        WHEN pc.prefix_count > pm.median_prefix_count * 0.9 THEN 'F'
        ELSE 'P'
    END AS peer_tag
FROM filtered_peers pc
JOIN prefix_medians pm 
ON pc.dump = pm.dump AND pc.prefix_ipversion = pm.prefix_ipversion;