[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "246d8a46-54fb-4891-b6c6-a0f96a6a126f"]}[Data]{[lakehouseId = "87c0302d-fecc-486b-9d67-943187cfd2a4"]}[Data];
[BindToDefaultDestination = true]
shared #"NA-EST2025-POP_unpivoted csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "246d8a46-54fb-4891-b6c6-a0f96a6a126f"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "87c0302d-fecc-486b-9d67-943187cfd2a4"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "Population"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "NA-EST2025-POP_unpivoted.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 5, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Year", Int64.Type}, {"Month", type date}, {"Date", type date}, {"Population_Type", type text}, {"Population_Value", Int64.Type}}),
  #"Filtered rows" = Table.SelectRows(#"Changed column type", each ([Year] = 2024 or [Year] = 2025 or [Year] = 2026)),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Filtered rows", {{"Month", type date}}),
  #"Calculated start of month" = Table.TransformColumns(#"Changed column type 1", {{"Month", each Date.StartOfMonth(_), type nullable date}}),
  #"Extracted month name" = Table.TransformColumns(#"Calculated start of month", {{"Month", each Date.MonthName(_), type nullable text}}),
  #"Filtered rows 1" = Table.SelectRows(#"Extracted month name", each ([Population_Type] = "Civilian Population"))
in
  #"Filtered rows 1";
