CREATE TABLE [dbo].[auto_dim_location] (

	[state_code] varchar(8000) NULL, 
	[cbsa_code] varchar(8000) NULL, 
	[county_code] varchar(8000) NULL, 
	[site_number] varchar(8000) NULL, 
	[latitude] float NULL, 
	[longitude] float NULL, 
	[city] varchar(8000) NULL, 
	[state_name] varchar(8000) NULL, 
	[county_name] varchar(8000) NULL, 
	[cbsa_name] varchar(8000) NULL, 
	[population] varchar(8000) NULL, 
	[location_key] int NULL, 
	[region] varchar(8000) NULL
);