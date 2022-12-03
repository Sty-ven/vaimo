WITH created_table AS
 (SELECT 
 date,
 name as location_name,
 dawn,
 dusk
 from `awesome-highway-358007.Daily_plan.Vaimo`),
 
formatted_table AS
(SELECT
 date,
 location_name,
  
FORMAT_TIMESTAMP(
    '%H:%M:%S',
    PARSE_TIMESTAMP('%I:%M:%S %p', created_table.dawn)
  ) AS dawn,
  FORMAT_TIMESTAMP(
    '%H:%M:%S',
    PARSE_TIMESTAMP('%I:%M:%S %p', created_table.dusk)
  ) AS dusk
  FROM created_table)

SELECT
  date,
  location_name,
  TIME_DIFF(TIME_ADD(CAST(formatted_table.dawn AS TIME),INTERVAL 12 HOUR), TIME_SUB(CAST(LAG(dusk) OVER(PARTITION BY location_name ORDER BY dusk) AS TIME),INTERVAL 12 HOUR), MINUTE) AS darkness_minute,
  TIME_DIFF(CAST(formatted_table.dusk AS TIME), CAST(formatted_table.dawn AS TIME), MINUTE) as daylight_minute
  
FROM formatted_table