with joining_location_date as (
		select *
		from {{ref('prep_forecast_day')}}
		left join {{ref('staging_location')}}
		using (city, region, country)
		)
		
, ordered_features as (		
		select date
            ,city
            ,region
            ,country
            ,time_epoch
            ,date_time
            ,is_day
            ,time
            ,hour
            ,month_of_year
            ,day_of_week
            ,condition_text
            ,condition_icon
            ,concat('&nbsp;&nbsp;&nbsp;&nbsp;![weather_icon](',condition_icon,'?width=35)') as condition_icon_md
            ,condition_code
            ,temp_c
            ,wind_kph
            ,wind_degree
            ,wind_dir
            ,pressure_mb
            ,precip_mm
            ,snow_cm
            ,humidity
            ,cloud
            ,feelslike_c
            ,windchill_c
            ,heatindex_c
            ,dewpoint_c
            ,will_it_rain
            ,chance_of_rain
            ,will_it_snow
            ,chance_of_snow
            ,vis_km
            ,gust_kph
            ,uv
		from joining_location_date)
		
select * from ordered_features
