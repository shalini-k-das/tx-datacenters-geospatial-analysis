SELECT 
    d.datacenter,
    d.county,
    d.operator,
    t."VOLTAGE" AS voltage_kv,
    t."OWNER" AS transmission_owner,
    ROUND((ST_Distance(d.geometry::geography, t.geometry::geography) / 1000)::numeric, 2) AS distance_km
FROM datacenters d
JOIN transmission_lines t 
    ON ST_DWithin(d.geometry::geography, t.geometry::geography, 10000)