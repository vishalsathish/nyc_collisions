SELECT borough, total_crashes
FROM crashes_by_borough
ORDER BY total_crashes DESC;

SELECT year, month, total_crashes
FROM monthly_trends
ORDER BY year, month;

SELECT hour, total_crashes
FROM crash_by_hour
ORDER BY total_crashes DESC
LIMIT 5;

SELECT factor, count
FROM top_factors
ORDER BY count DESC
LIMIT 10;