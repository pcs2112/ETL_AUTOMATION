-- ROW_COUNT: <ROW_COUNT>, ROW_MIN_DATE: <ROW_MIN_DATE>, ROW_MAX_DATE: <ROW_MAX_DATE>, MONTH_COUNT: <MONTH_COUNT>
CREATE NONCLUSTERED INDEX [IDX_<TABLE_NAME>_<COLUMN_NAME>]
	ON [<SCHEMA_NAME>].[<TABLE_NAME>]
	([<COLUMN_NAME>] ASC)
	WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80)
	ON [PRIMARY]

<MESSAGE>
GO
