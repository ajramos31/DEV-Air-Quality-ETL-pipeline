CREATE TABLE [dbo].[fact_daily_air_quality] (

	[date_key] bigint NULL, 
	[location_key] bigint NULL, 
	[parameter_key] bigint NULL, 
	[poc] bigint NULL, 
	[method_key] bigint NULL, 
	[arithmetic_mean] float NULL, 
	[first_max_value] float NULL, 
	[first_max_hour] bigint NULL, 
	[aqi] bigint NULL, 
	[observation_count] bigint NULL, 
	[observation_percent] float NULL, 
	[aqi_category] varchar(8000) NULL, 
	[exceeds_standard] bit NULL
);