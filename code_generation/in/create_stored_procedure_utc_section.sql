DECLARE @dtEST DATETIME;
SET @dtEST = getdate();
DECLARE @dtUTC DATETIME;
SET @dtUTC = GETUTCDATE();
DECLARE @HourDiff   integer;

set @HourDiff =  datediff( hour, @dtEST, @dtUTC);
DECLARE      @ReportTimeShift           varchar(80);
SET @ReportTimeShift = 'UTC time shift : ' + cast(@HourDiff as varchar(4)) + ' hours';

--  NOTE, this is very important,  I need to move this by 4 hours, because I3 has a 4 hour difference in TIME because of UTC

DECLARE @SPERIOD_START_DTTM DATETIME2;
SET @SPERIOD_START_DTTM =  dateadd(HH, @HourDiff , @PERIOD_START_DTTM);

DECLARE @SPERIOD_END_DTTM DATETIME2;
SET @SPERIOD_END_DTTM = dateadd(HH, @HourDiff , @PERIOD_END_DTTM);
