CREATE TABLE [dbo].[auto_dim_date] (

	[date] date NULL, 
	[year] int NULL, 
	[month] int NULL, 
	[month_name] varchar(8000) NULL, 
	[day] int NULL, 
	[day_of_week] int NULL, 
	[day_name] varchar(8000) NULL, 
	[quarter] int NULL, 
	[is_weekend] bit NULL, 
	[date_key] int NULL
);