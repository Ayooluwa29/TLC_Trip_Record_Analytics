SELECT DISTINCT(payment_type),
	CASE
		WHEN payment_type = 0 THEN 'Flex Fare trip'
		WHEN payment_type = 1 THEN 'Credit card'
		WHEN payment_type = 2 THEN 'Cash'
		WHEN payment_type = 3 THEN 'No charge'
		WHEN payment_type = 4 THEN 'Dispute'
		WHEN payment_type = 5 THEN 'Unknown'
		WHEN payment_type = 6 THEN 'Voided trip'
	ELSE 'N/A'
	END AS payment_type_id
FROM bronze.tlc_trip_yellow_taxi_2022