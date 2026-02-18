CREATE TABLE [dbo].[dim_date] (

	[date_key] bigint NULL, 
	[date] date NULL, 
	[year] bigint NULL, 
	[month] bigint NULL, 
	[month_name] varchar(8000) NULL, 
	[day] bigint NULL, 
	[day_of_week] bigint NULL, 
	[day_name] varchar(8000) NULL, 
	[quarter] bigint NULL, 
	[is_weekend] bit NULL
);