[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Append"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Fabric.Warehouse([CreateNavigationProperties = false]){[workspaceId = "246d8a46-54fb-4891-b6c6-a0f96a6a126f"]}[Data]{[warehouseId = "8abb56af-3e98-4d1f-af3b-d5629ab273fb"]}[Data];
[BindToDefaultDestination = true]
shared dim_date = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "246d8a46-54fb-4891-b6c6-a0f96a6a126f"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "87c0302d-fecc-486b-9d67-943187cfd2a4"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "DummyData"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "dim_date.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 10, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"date_key", Int64.Type}, {"date", type date}, {"year", Int64.Type}, {"month", Int64.Type}, {"month_name", type text}, {"day", Int64.Type}, {"day_of_week", Int64.Type}, {"day_name", type text}, {"quarter", Int64.Type}, {"is_weekend", type logical}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared dim_location = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "246d8a46-54fb-4891-b6c6-a0f96a6a126f"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "87c0302d-fecc-486b-9d67-943187cfd2a4"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "DummyData"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "dim_location.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 13, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"location_key", Int64.Type}, {"state_code", Int64.Type}, {"county_code", Int64.Type}, {"site_number", Int64.Type}, {"state_name", type text}, {"county_name", type text}, {"city", type text}, {"cbsa_code", Int64.Type}, {"cbsa_name", type text}, {"latitude", type number}, {"longitude", type number}, {"population", Int64.Type}, {"region", type text}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared dim_method = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "246d8a46-54fb-4891-b6c6-a0f96a6a126f"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "87c0302d-fecc-486b-9d67-943187cfd2a4"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "DummyData"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "dim_method.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 3, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"method_key", Int64.Type}, {"method_code", type text}, {"method_name", type text}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared dim_parameter = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "246d8a46-54fb-4891-b6c6-a0f96a6a126f"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "87c0302d-fecc-486b-9d67-943187cfd2a4"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "DummyData"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "dim_parameter.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 5, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"parameter_key", Int64.Type}, {"parameter_code", Int64.Type}, {"parameter_name", type text}, {"unit_of_measurement", type text}, {"category", type text}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared fact_daily_air_quality = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "246d8a46-54fb-4891-b6c6-a0f96a6a126f"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "87c0302d-fecc-486b-9d67-943187cfd2a4"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "DummyData"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "fact_daily_air_quality.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 13, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"date_key", Int64.Type}, {"location_key", Int64.Type}, {"parameter_key", Int64.Type}, {"poc", Int64.Type}, {"method_key", Int64.Type}, {"arithmetic_mean", type number}, {"first_max_value", type number}, {"first_max_hour", Int64.Type}, {"aqi", Int64.Type}, {"observation_count", Int64.Type}, {"observation_percent", type number}, {"aqi_category", type text}, {"exceeds_standard", type logical}}),
  #"Kept range of rows" = Table.Range(#"Changed column type", 0, 200)
in
  #"Kept range of rows";
