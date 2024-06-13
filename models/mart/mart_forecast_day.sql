with joining_day_location as (
		select * from {{source("prep_forecast_day")}} 
		left join {{source("staging_location")}}
		using (city,region,country)
	),
	
ordered_features as
	(select date
            ,day_of_month
            ,month_of_year
            ,year
            ,day_of_week
            ,date_part('week',date) as week_of_year
            ,year_and_week
            ,city
            ,region
            ,country
            ,lat
            ,lon
            ,timezone_id
            ,max_temp_c
            ,min_temp_c
            ,avg_temp_c
            ,total_precip_mm
            ,total_snow_cm
            ,avg_humidity
            ,daily_will_it_rain
            ,daily_chance_of_rain
            ,daily_will_it_snow
            ,daily_chance_of_snow
            ,condition_text
            ,condition_icon
			,concat('&nbsp;&nbsp;&nbsp;&nbsp;![weather_icon](',condition_icon,'?width=35)') as condition_icon_md
            ,condition_code
            ,max_wind_kph
            ,avg_vis_km
            ,uv,
	(case 
		when sunrise='No sunrise' then null
		else sunrise
	end)::TIME as sunrise_n,
	(case 
		when sunset='No sunset' then null
		else sunset
	end)::TIME as sunset_n,
	(case 
		when moonrise='No moonrise' then null
		else moonrise
	end)::TIME as moonrise_n,
	(case 
		when moonset='No moonset' then null
		else moonset
	end)::TIME as moonset_n
	,moon_phase
    ,moon_illumination
	from joining_day_location
)

select * from ordered_features;

