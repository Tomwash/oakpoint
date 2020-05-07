Declare @TableView TABLE (
    ID INT IDENTITY(1,1) ,
    [col_href] varchar(8000) ,
    [col_guarantor_id] varchar(8000) ,
    [col_amount_less_than_30] varchar(8000) ,
    [col_amount_between_30_60] varchar(8000) ,
    [col_amount_between_60_90] varchar(8000) ,
    [col_amount_greater_than_90] varchar(8000) ,
    [col_practice_id] varchar(8000) ,
    [col_entry_id] varchar(8000) ,
    [col_current_date] varchar(8000) ,
    [col_guarantor] varchar(8000) ,
    [col_practice] varchar(8000) );
declare @json nvarchar(max) = ( SELECT CAST(BulkColumn AS NVARCHAR(MAX)) AS JsonData
FROM OPENROWSET(BULK 'oakpoint-data/D18336/accounts_receivables.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob );

INSERT INTO @TableView
    ([col_href],[col_guarantor_id],[col_amount_less_than_30],[col_amount_between_30_60],[col_amount_between_60_90],[col_amount_greater_than_90],[col_practice_id],[col_entry_id],[col_current_date],[col_guarantor],[col_practice] )
SELECT
    *
FROM OPENJSON(@json) WITH ([col_href] varchar(8000) '$.href',[col_guarantor_id] varchar(8000) '$.guarantor_id',[col_amount_less_than_30] varchar(8000) '$.amount_less_than_30',[col_amount_between_30_60] varchar(8000) '$.amount_between_30_60',[col_amount_between_60_90] varchar(8000) '$.amount_between_60_90',[col_amount_greater_than_90] varchar(8000) '$.amount_greater_than_90',[col_practice_id] varchar(8000) '$.practice_id',[col_entry_id] varchar(8000) '$.entry_id',[col_current_date] varchar(8000) '$.current_date',[col_guarantor] varchar(8000) '$.guarantor',[col_practice] varchar(8000) '$.practice' )


SELECT *
FROM @TableView

MERGE claims original 
USING @TableView modified 
ON (original.col_href = modified.col_href) 
WHEN MATCHED THEN 
    UPDATE SET 
        original.[col_href] = modified.[col_href],
        original.[col_guarantor_id] = modified.[col_guarantor_id],
        original.[col_amount_less_than_30] = modified.[col_amount_less_than_30],
        original.[col_amount_between_30_60] = modified.[col_amount_between_30_60],original.[col_amount_between_60_90] = modified.[col_amount_between_60_90],original.[col_amount_greater_than_90] = modified.[col_amount_greater_than_90],original.[col_practice_id] = modified.[col_practice_id],original.[col_entry_id] = modified.[col_entry_id],original.[col_current_date] = modified.[col_current_date],original.[col_guarantor] = modified.[col_guarantor],original.[col_practice] = modified.[col_practice] WHEN NOT MATCHED BY TARGET THEN INSERT ([col_href],[col_guarantor_id],[col_amount_less_than_30],[col_amount_between_30_60],[col_amount_between_60_90],[col_amount_greater_than_90],[col_practice_id],[col_entry_id],[col_current_date],[col_guarantor],[col_practice] ) VALUES ( SELECT * FROM OPENJSON(@json) WITH ([col_href] varchar(8000) '$.href',[col_guarantor_id] varchar(8000) '$.guarantor_id',[col_amount_less_than_30] varchar(8000) '$.amount_less_than_30',[col_amount_between_30_60] varchar(8000) '$.amount_between_30_60',[col_amount_between_60_90] varchar(8000) '$.amount_between_60_90',[col_amount_greater_than_90] varchar(8000) '$.amount_greater_than_90',[col_practice_id] varchar(8000) '$.practice_id',[col_entry_id] varchar(8000) '$.entry_id',[col_current_date] varchar(8000) '$.current_date',[col_guarantor] varchar(8000) '$.guarantor',[col_practice] varchar(8000) '$.practice' ) ) ) 



select * from prescriptions_dental where office_id = 'D22072' order by CAST(col_prescription_id as INT)

SELECT
    col_href, COUNT(*)
FROM
    prescriptions_dental
WHERE
    office_id = 'D22072'
GROUP BY
    col_href
HAVING 
    COUNT(*) > 1