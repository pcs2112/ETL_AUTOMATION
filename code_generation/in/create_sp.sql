IF EXISTS(SELECT *
          FROM sys.objects so
                 JOIN sys.schemas ss on (so.schema_id = ss.schema_id)
          WHERE so.type = 'P'
            AND so.name = '<STORED_PROCEDURE_NAME>'
            and ss.name = '<TARGET_SCHEMA>')
  DROP PROCEDURE <TARGET_SCHEMA>.<STORED_PROCEDURE_NAME>
GO

EXEC MWH.ETL_SOURCE_TARGET_DETAIL_MGR  'ADD', '<SOURCE_DATA_MART>', '<SOURCE_SERVER>', '<SOURCE_DATABASE>',
                                       '<SOURCE_SCHEMA>', '<SOURCE_TABLE>', '<SOURCE_TABLE_SEARCH_COLUMN>',
                                       '<TARGET_SCHEMA>', '<TARGET_TABLE>', '<SOURCE_TABLE_SEARCH_COLUMN>',
                                       '<STORED_PROCEDURE_NAME>', 1
GO


CREATE PROCEDURE <TARGET_SCHEMA>.<STORED_PROCEDURE_NAME>
  @PERIOD_START_DTTM DATETIME2,
  @PERIOD_END_DTTM DATETIME2
AS

<UTC_SECTION>

  DECLARE @END_DATETIME DATETIME2;
  DECLARE @START_DATETIME DATETIME2;
  SET @START_DATETIME = sysdatetime();

  DECLARE @vSTART_RTNCODE INTEGER = -1;
  DECLARE @vFINISHED_RTNCODE INTEGER = -1;

  DECLARE @rtn_Insert_Cnt INTEGER = 0;
  DECLARE @rtn_Update_Cnt INTEGER = 0;
  DECLARE @rtn_Delete_Cnt INTEGER = 0;

  DECLARE @vPERIOD_START_DTTM varchar(27);
  DECLARE @vPERIOD_END_DTTM varchar(27);

  set @vPERIOD_START_DTTM = cast(@PERIOD_START_DTTM as varchar(27));
  set @vPERIOD_END_DTTM = cast(@PERIOD_END_DTTM as varchar(27));


  DECLARE @SummaryOfChanges TABLE(Change VARCHAR(20));

  DECLARE @LOAD_HIST_PKID INTEGER;
  DECLARE @LOAD_HIST_DUMMY INTEGER;

  DECLARE @ERR INTEGER = 0;
  DECLARE @ErrorSeverity INTEGER;
  DECLARE @ErrorState INTEGER;
  DECLARE @ErrorProcedure nvarchar(128);
  DECLARE @ErrorLine INTEGER;
  DECLARE @ErrorMessage nvarchar(4000);
  DECLARE @TryCatchError_ID INTEGER = 0;


  DECLARE @My_SP_NAME varchar(50);
  SET @My_SP_NAME = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID);

  --  The Source Names are either LOCAL or REMOTE,  when REMOTE, manually enter the nameed automatically
  --  Using the FUNCTION below for LOCAL, and when we move to NEW server, these will be updat
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


  IF OBJECT_ID('tempdb..#<SOURCE_TABLE>') IS NOT NULL
    DROP TABLE #<SOURCE_TABLE>;

  CREATE TABLE # <SOURCE_TABLE> (
<SOURCE_TABLE_COLUMN_DEFINITION>
  );

  INSERT into # <SOURCE_TABLE>(
<SOURCE_TABLE_COLUMN_NAMES>
      )
  SELECT
<SOURCE_TABLE_COLUMN_NAMES>
      FROM [<SOURCE_SERVER>].[<SOURCE_DATABASE>].[<SOURCE_SCHEMA>].[<SOURCE_TABLE>] with (nolock)
      WHERE <SOURCE_TABLE_SEARCH_COLUMN> between @<PERIOD_START_PREFIX>PERIOD_START_DTTM and @ <PERIOD_END_PREFIX> PERIOD_END_DTTM <SOURCE_TABLE_SEARCH_CONDITION>;


  MERGE <TARGET_SCHEMA>.<TARGET_TABLE> AS target
  USING (SELECT
<SOURCE_TABLE_COLUMN_NAMES>
  FROM #<SOURCE_TABLE>
  ) as source (
<SOURCE_TABLE_COLUMN_NAMES>
  )

  ON (target.<SOURCE_TABLE_PRIMARY_KEY> = source.<SOURCE_TABLE_PRIMARY_KEY>)
  <MATCH_SECTION>
  UPDATE SET
      MSTR_LOAD_ID = @LOAD_HIST_PKID,
<TARGET_TABLE_UPDATE_VALUES>
      WHEN NOT MATCHED THEN
  INSERT (
      MSTR_LOAD_ID,
<TARGET_TABLE_INSERT_COLUMNS>

      ) VALUES (
      @LOAD_HIST_PKID,
<TARGET_TABLE_INSERT_VALUES>
      )
      OUTPUT $ action INTO @SummaryOfChanges;

  SELECT @rtn_Insert_Cnt = COUNT(*)
  FROM @SummaryOfChanges
  WHERE Change = 'INSERT'
  GROUP BY Change;

  SELECT @rtn_Update_Cnt = COUNT(*)
  FROM @SummaryOfChanges
  WHERE Change = 'UPDATE'
  GROUP BY Change;

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
                              @My_SP_NAME, @vPERIOD_START_DTTM, @vPERIOD_END_DTTM, @rtn_Insert_Cnt, @rtn_Update_Cnt,
                              @rtn_Delete_Cnt, @TryCatchError_ID, 0, '', @LOAD_HIST_DUMMY OUTPUT;


  IF OBJECT_ID('tempdb..#<SOURCE_TABLE>') IS NOT NULL
    DROP TABLE #<SOURCE_TABLE>;

  RETURN

GO
