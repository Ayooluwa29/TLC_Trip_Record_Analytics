SELECT DISTINCT(CAST("RatecodeID" AS BIGINT)) RatecodeID,
	CASE
		WHEN "RatecodeID" = 1 THEN 'Standard rate'
		WHEN "RatecodeID" = 2 THEN 'JFK'
		WHEN "RatecodeID" = 3 THEN 'Newark'
		WHEN "RatecodeID" = 4 THEN 'Nassau or Westchester'
		WHEN "RatecodeID" = 5 THEN 'Negotiated fare'
		WHEN "RatecodeID" = 6 THEN 'Group ride'
		WHEN "RatecodeID" = 99 THEN 'Null/unknown'
	ELSE 'N/A'
	END AS Ratecode
FROM bronze.tlc_trip_yellow_taxi_2022