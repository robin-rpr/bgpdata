-- 
-- BGPDATA - BGP Data Collection and Analytics Service
-- 
-- This software is part of the BGPDATA project, which is designed to collect, process, and analyze BGP data from various sources.
-- It helps researchers and network operators get insights into their network by providing a scalable and reliable way to analyze and inspect historical and live BGP data from RIPE NCC RIS.
-- 
-- Author: Robin Röper
-- 
-- © 2024 BGPDATA. All rights reserved.
-- 
-- Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
-- 1. Redistributions of source code must retain the above copyright notice, this list of conditions, and the following disclaimer.
-- 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions, and the following disclaimer in the documentation and/or other materials provided with the distribution.
-- 3. Neither the name of BGPDATA nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
-- 
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
-- 

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