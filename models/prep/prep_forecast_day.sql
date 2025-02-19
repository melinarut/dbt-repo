WITH forecast_day_data AS (
    SELECT * 
    FROM {{ref('staging_forecast_day')}}
),
add_features AS (
    SELECT *
        ,DATE_PART('day',date) AS day_of_month -- day of month as a number
        ,TO_CHAR(date, 'Month') AS month_of_year -- month name as a text
        ,DATE_PART('year',date) AS year -- year as a number
        ,TO_CHAR(date, 'Day') AS day_of_week -- weekday name as text
        ,DATE_PART('week', date) -- calender week number as number
        ,concat(DATE_PART('year',date),'-',DATE_PART('week', date)) AS year_and_week -- year-calenderweek as text like '2024-43'

    FROM forecast_day_data
)
SELECT *
FROM add_features
