CREATE TABLE <TARGET_SCHEMA>.<TARGET_TABLE> (
    ID INT NOT NULL IDENTITY (1,1),
    INSERT_DTTM DATETIME CONSTRAINT <TARGET_TABLE>_INSERT_DTTM_DF DEFAULT getdate() NOT NULL,
    UPDATE_DTTM DATETIME CONSTRAINT <TARGET_TABLE>_UPDATE_DTTM_DF DEFAULT getdate() NOT NULL,
    LST_MOD_USER VARCHAR(80) CONSTRAINT <TARGET_TABLE>_LST_MOD_USER_DF DEFAULT user_name() NOT NULL,
    MSTR_LOAD_ID INT CONSTRAINT <TARGET_TABLE>_MSTR_LOAD_ID_DF DEFAULT (-1) NOT NULL,
    ACTIVE_FLAG SMALLINT CONSTRAINT <TARGET_TABLE>_ACTIVE_FLG_DF DEFAULT 1 NOT NULL,
<TARGET_TABLE_COLUMNS>
    CONSTRAINT PK_<TARGET_TABLE> PRIMARY KEY NONCLUSTERED
    (
<TARGET_TABLE_KEY_COLUMNS>
    )
    WITH (
        PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON
    ) ON <PARTITION_NAME>
) ON <INDEX_PARTITION_NAME>

GO

CREATE TRIGGER <TARGET_SCHEMA>.<TARGET_TABLE>_TRIG
ON <TARGET_SCHEMA>.<TARGET_TABLE>
AFTER UPDATE
AS  BEGIN
    UPDATE <TARGET_SCHEMA>.<TARGET_TABLE>
    SET UPDATE_DTTM = GETDATE()
    WHERE ID IN (SELECT DISTINCT ID FROM INSERTED)
END
GO
