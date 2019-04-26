	select @SRC_MISS_Cnt = count(*)
	from [<TARGET_DATABASE>].[<TARGET_SCHEMA>].[<TARGET_TABLE>] trg with (nolock)
	where not exists (
	    select *
	    FROM <SOURCE_SERVER_SELECT>[<SOURCE_DATABASE>].[<SOURCE_SCHEMA>].[<SOURCE_TABLE>] src with(nolock)
	    where 1 = 1 AND <TARGET_PK_CONDITION>
	);

	SET @ERR1 = @@ERROR;

	select @TRG_MISS_Cnt = count(*), @MIN_MISS_DTTM = MIN(<SOURCE_TABLE_SEARCH_COLUMN>)
	from <SOURCE_SERVER_SELECT>[<SOURCE_DATABASE>].[<SOURCE_SCHEMA>].[<SOURCE_TABLE>] trg with (nolock)
	where not exists(
	    select *
	    from [<TARGET_DATABASE>].[<TARGET_SCHEMA>].[<TARGET_TABLE>] src with (nolock)
	    where 1 = 1 AND <SOURCE_PK_CONDITION>
	);

	SET @ERR2 = @@ERROR;
