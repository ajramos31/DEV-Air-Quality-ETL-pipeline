CREATE TABLE [dbo].[auto_fact_daily_air_quality] (

	[date_key] int NULL, 
	[location_key] int NULL, 
	[parameter_key] int NULL, 
	[poc] int NULL, 
	[method_key] int NULL, 
	[arithmetic_mean] float NULL, 
	[first_max_value] float NULL, 
	[first_max_hour] int NULL, 
	[aqi] int NULL, 
	[observation_count] int NULL, 
	[observation_percent] float NULL, 
	[aqi_category] varchar(8000) NULL, 
	[exceeds_standard] bit NULL
);