WITH joining_day_location AS (
        SELECT * FROM {{ref('prep_forecast_day')}}
        LEFT JOIN {{ref('staging_location')}}
        USING(city,region,country)
),

aggregations_features AS (
        SELECT 
            year_and_week  -- grouping on
            ,city           -- grouping on
            ,country        -- grouping on
            ,lat            -- grouping on
            ,lon            -- grouping on
            ,timezone_id    -- grouping on
            ,CASE
				WHEN condition_text ='Sunny' THEN 'sunny_days'
				WHEN condition_text in ('Overcast','Cloudy','Fog','Mist') THEN 'cloudy_days'
				WHEN condition_text like 'Thundery outbreaks possible' then 'rainy_days'
				WHEN condition_text like '%cloud%' then 'cloudy_days'
				WHEN condition_text like '%rain%' then 'rainy_days'
				WHEN condition_text like '%drizzle%' then 'rainy_days'
				ELSE 'other_days' 
                end as weather_bucket
            ,MAX(max_temp_c) AS max_temp_c
            ,MIN(min_temp_c) AS min_temp_c
            ,AVG(avg_temp_c) AS avg_temp_c
            ,SUM(total_precip_mm) AS total_precip_mm
            ,SUM(total_snow_cm) AS total_snow_cm
            ,AVG(avg_humidity) AS avg_humidity
            ,SUM(daily_will_it_rain) AS will_it_rain_days
            ,AVG(daily_chance_of_rain) AS daily_chance_of_rain_avg
            ,SUM(daily_will_it_snow) AS will_it_snow_days
            ,AVG(daily_chance_of_snow) AS daily_chance_of_snow_avg
        FROM joining_day_location
    	GROUP BY (year_and_week, city, country, lat, lon, timezone_id, weather_bucket)
    	ORDER BY city
    	)
    	
SELECT * FROM aggregations_features