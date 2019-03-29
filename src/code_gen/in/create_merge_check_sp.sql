--  <STORED_PROCEDURE_SCHEMA>.<STORED_PROCEDURE_NAME>

/*
exec  <STORED_PROCEDURE_SCHEMA>.<STORED_PROCEDURE_NAME> '2017-11-18 00:00:00.000',  '2017-11-20 14:06:23.060'

exec  <STORED_PROCEDURE_SCHEMA>.<STORED_PROCEDURE_NAME> '2019-03-26 00:00:00.000',  '2019-03-27 00:00:00.000', '2001-01-01'

select top 10 * from [MWH].[ETL_HISTORY] with(nolock) where [CALLING_PROC] = '<STORED_PROCEDURE_SCHEMA>.<STORED_PROCEDURE_NAME>' order by ID desc
*/

EXEC MWH.MNG_ETL_CONTROL_MANAGER  'ADD', 'TESTING_DIMENSIONS', '<STORED_PROCEDURE_SCHEMA>.<STORED_PROCEDURE_NAME>', 15, 1440, 1, 1,
                                  '09:00:00.000', '17:00:00.000', 'REPLICA', 1
GO

IF EXISTS(SELECT *
          FROM sys.objects so
	               JOIN sys.schemas ss on (so.schema_id = ss.schema_id)
          WHERE so.type = 'P'
		        AND so.name = '<STORED_PROCEDURE_NAME>'
		        and ss.name = '<STORED_PROCEDURE_SCHEMA>')
	DROP PROCEDURE <STORED_PROCEDURE_SCHEMA>.<STORED_PROCEDURE_NAME>
GO

SET NOCOUNT ON;
GO

CREATE PROCEDURE <STORED_PROCEDURE_SCHEMA>.<STORED_PROCEDURE_NAME>
		@PERIOD_START_DTTM DATETIME2,
		@PERIOD_END_DTTM DATETIME2,
		@GetEarliestDTTM DATETIME = '1990-01-01'
AS

	IF (@GetEarliestDTTM != '1990-01-01')
		begin
			select @PERIOD_START_DTTM = min(<SOURCE_TABLE_SEARCH_COLUMN>)
			FROM [<SOURCE_SERVER>].[<SOURCE_DATABASE>].[<SOURCE_SCHEMA>].[<SOURCE_TABLE>] with(nolock);
		END

	SET @PERIOD_END_DTTM = DATEADD(hh, 5, @PERIOD_END_DTTM);

	DECLARE @SRC_MISS_Cnt INTEGER = 0;
	DECLARE @TRG_MISS_Cnt INTEGER = 0;
	DECLARE @ERR1 INT;
	DECLARE @ERR2 INT;
	DECLARE @RTN_ERR INT;
	DECLARE @END_DATETIME DATETIME2;
	DECLARE @START_DATETIME DATETIME2;

	DECLARE @vPERIOD_START_DTTM varchar(27);
	DECLARE @vPERIOD_END_DTTM varchar(27);

	SET @vPERIOD_START_DTTM = cast(@PERIOD_START_DTTM as varchar(27));
	SET @vPERIOD_END_DTTM = cast(@PERIOD_END_DTTM as varchar(27));

	DECLARE @LOAD_HIST_PKID INTEGER;
	DECLARE @LOAD_HIST_DUMMY INTEGER;

	SET @START_DATETIME = sysdatetime();

	DECLARE @MIN_MISS_DTTM_MESS VARCHAR(400) = '';
	DECLARE @MIN_MISS_DTTM DATETIME2;
	DECLARE @ERR INTEGER = 0;
	DECLARE @ErrorSeverity INTEGER;
	DECLARE @ErrorState INTEGER;
	DECLARE @ErrorProcedure nvarchar(128);
	DECLARE @ErrorLine INTEGER;
	DECLARE @ErrorMessage nvarchar(4000);
	DECLARE @TryCatchError_ID INTEGER = 0;

	DECLARE @My_SP_NAME varchar(50);
	SET @My_SP_NAME = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID);

	--  The Source Names are either LOCAL or REMOTE,  when REMOTE, manually enter the named automatically
	--  Using the FUNCTION below for LOCAL, and when we move to NEW server, these will be updated.

	DECLARE @Source_Server_Name varchar(60);
	SET @Source_Server_Name = '<SOURCE_SERVER>';

	DECLARE @Source_DB_Name varchar(40);
	SET @Source_DB_Name = '<SOURCE_DATABASE>';

	DECLARE @Source_Schema_Name varchar(40);
	SET @Source_Schema_Name = '<SOURCE_SCHEMA>';

	DECLARE @Source_Table_Name varchar(60);
	SET @Source_Table_Name = '<SOURCE_TABLE>';

	---------------------------------------------------------------------------------------------------------

	DECLARE @Target_Schema_Name varchar(40);
	SET @Target_Schema_Name = '<TARGET_SCHEMA>';

	DECLARE @Target_Table_Name varchar(60);
	SET @Target_Table_Name = '<TARGET_TABLE>';

	EXEC MWH.MNG_LOAD_HISTORY   'START', @START_DATETIME, 0, @Source_Server_Name, @Source_DB_Name, @Source_Schema_Name,
	                            @Source_Table_Name, @Target_Schema_Name, @Target_Table_Name, @My_SP_NAME,
	                            @vPERIOD_START_DTTM, @vPERIOD_END_DTTM, 0, 0, 0, 0, 0, '', @LOAD_HIST_PKID OUTPUT;

	BEGIN TRY

	select @SRC_MISS_Cnt = count(*)
	from [TARGET_DATABASE].[<TARGET_SCHEMA>].[TARGET_TABLE] with (nolock)
	where <TARGET_TABLE_PRIMARY_KEY> not in (select <TARGET_TABLE_PRIMARY_KEY>
	                               FROM [<SOURCE_SERVER>].[<SOURCE_DATABASE>].[<SOURCE_SCHEMA>].[<SOURCE_TABLE>] with(nolock)
	                               where <SOURCE_TABLE_SEARCH_COLUMN> between @PERIOD_START_DTTM and @PERIOD_END_DTTM)
	                               and <SOURCE_TABLE_SEARCH_COLUMN> between @PERIOD_START_DTTM and @PERIOD_END_DTTM;
	SET @ERR1 = @@ERROR;

	select @TRG_MISS_Cnt = count(*), @MIN_MISS_DTTM = MIN(<SOURCE_TABLE_SEARCH_COLUMN>)
	from [<SOURCE_SERVER>].[<SOURCE_DATABASE>].[<SOURCE_SCHEMA>].[SOURCE_TABLE] t with (nolock)
	where not exists(select *
	                 from [TARGET_DATABASE].[<TARGET_SCHEMA>].[TARGET_TABLE] s with (nolock)
	                 where s.<TARGET_TABLE_PRIMARY_KEY> = t.<TARGET_TABLE_PRIMARY_KEY>)
		and <SOURCE_TABLE_SEARCH_COLUMN> between @PERIOD_START_DTTM and @PERIOD_END_DTTM;

	SET @ERR2 = @@ERROR;
	SET @RTN_ERR = [MWH].[qfst_non_zero](@ERR1, @ERR2);

	IF (@TRG_MISS_Cnt > 0 and @ERR2 = 0)
		BEGIN
			SET @MIN_MISS_DTTM_MESS =
			@My_SP_NAME + ' : MIN DTTM MISSING FROM TARGET : ' + cast(@MIN_MISS_DTTM as varchar(27)) +
			' in SEARCH date range ' + @vPERIOD_START_DTTM + ' and ' + @vPERIOD_END_DTTM;
		END

	END TRY
	BEGIN CATCH
	SELECT @ERR = ERROR_NUMBER(),
	       @ErrorSeverity = ERROR_SEVERITY(),
	       @ErrorState = ERROR_STATE(),
	       @ErrorProcedure = ERROR_PROCEDURE(),
	       @ErrorLine = ERROR_LINE(),
	       @ErrorMessage = ERROR_MESSAGE();

	EXEC MWH.MERGE_ETL_TryCatchError_wRtn 'save error', @ERR, @ErrorSeverity, @ErrorState, @ErrorProcedure, @ErrorLine,
	                                      @ErrorMessage, @My_SP_NAME, @TryCatchError_ID OUTPUT;
	END CATCH;

	SET @END_DATETIME = sysdatetime();

	EXEC MWH.MNG_LOAD_HISTORY   'FINISHED', @END_DATETIME, @LOAD_HIST_PKID, @Source_Server_Name, @Source_DB_Name,
	                            @Source_Schema_Name, @Source_Table_Name, @Target_Schema_Name, @Target_Table_Name,
	                            @My_SP_NAME, @vPERIOD_START_DTTM, @vPERIOD_END_DTTM, @SRC_MISS_Cnt, @TRG_MISS_Cnt, 0,
	                            @TryCatchError_ID, 0, @MIN_MISS_DTTM_MESS, @LOAD_HIST_DUMMY OUTPUT;

	RETURN

GO
