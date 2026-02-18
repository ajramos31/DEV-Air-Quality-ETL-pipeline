CREATE TABLE [dbo].[dim_location] (

	[location_key] int NULL, 
	[state_code] varchar(255) NULL, 
	[county_code] varchar(255) NULL, 
	[site_number] int NULL, 
	[state_name] varchar(255) NULL, 
	[county_name] varchar(255) NULL, 
	[city] varchar(255) NULL, 
	[cbsa_code] varchar(255) NULL, 
	[cbsa_name] varchar(255) NULL, 
	[latitude] float NULL, 
	[longitude] float NULL, 
	[population] int NULL, 
	[region] varchar(255) NULL
);