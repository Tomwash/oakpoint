-- CREATE EXTERNAL DATA SOURCE OakpointDataV1
--  WITH ( TYPE = BLOB_STORAGE,
--         LOCATION = 'https://oakpointfunctionappv1.blob.core.windows.net',
--         CREDENTIAL= OakpointData2030);

--         CREATE DATABASE SCOPED CREDENTIAL OakpointData2030
-- WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
-- SECRET = 'sv=2019-10-10&ss=bfqt&srt=sco&sp=rwdlacup&se=2030-05-04T10:08:45Z&st=2020-05-04T02:08:45Z&spr=https&sig=aSbvoQ4Nz5Jz5Fc1jscw9I3YtS5y5WyImZvb%2FtudCeY%3D';

-- drop external data source OakpointDataV12030

DECLARE @columnsCreateTable nvarchar(max) = N'ID INT';
DECLARE @columns nvarchar(max) = N'';
DECLARE @schema nvarchar(max) = N'';
DECLARE @stm nvarchar(max);
DECLARE @tableName nvarchar(max) = 'claims';
DECLARE @office_id nvarchar(max);
DECLARE @practice_name NVARCHAR(max);
DECLARE @setCommand nvarchar(max);

declare @json nvarchar(max) = 
    (
        SELECT
    CAST(BulkColumn AS NVARCHAR(MAX)) AS JsonData
FROM
    OPENROWSET(BULK 'oakpoint-data/D18336/claims.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob
    );

SELECT
    @columnsCreateTable = CONCAT(@columnsCreateTable, N',', QUOTENAME('col_' + jd.[key])),
    @columns = CONCAT(@columns, N',', QUOTENAME('col_' + jd.[key])),
    -- @columns = CONCAT(@columns, N',', QUOTENAME('updated_at')),
    -- @columns = CONCAT(@columns, N',', QUOTENAME('office_id')),
    -- @columns = CONCAT(@columns, N',', QUOTENAME('practice_name')),

    -- @schema = CONCAT(@schema, N',', QUOTENAME('DATA'), N' varchar(max) '), 
    -- @schema = CONCAT(@schema, N',', QUOTENAME('updated_at'), N' datetime '), 
    -- @schema = CONCAT(@schema, N',', QUOTENAME('office_id'), N' varchar(8000) '),
    -- @schema = CONCAT(@schema, N',', QUOTENAME('practice_name'), N' varchar(8000) '),
    @schema = CONCAT(@schema, N',', QUOTENAME('col_' + jd.[key]), N' varchar(8000) ''$.', jd.[key], N''''),

    @setCommand = CONCAT(@setCommand, N',', 'original.[col_', jd.[key], '] = modified.[col_', jd.[key], ']')
FROM
    (SELECT TOP 1
        [Value]
    FROM OPENJSON(@json)) j
CROSS APPLY
   OPENJSON(j.[value]) jd;

-- Statement
SET @stm = CONCAT(
    -- Delcare Temp Table
    N'Declare @TableView TABLE ',
    N'( ',
    STUFF(@columns, 1, 1, N''),
    N'); ',
    -- Declare JSON Variable
    N'declare @json nvarchar(max) = ',
    N'  (',
    N'      SELECT',
    N'          CAST(BulkColumn AS NVARCHAR(MAX)) AS JsonData ',
    N'      FROM ',
    N'      OPENROWSET(BULK ''oakpoint-data/D18336/' + @tableName + '.json'', DATA_SOURCE = ''OakpointDataV1'', SINGLE_CLOB) AS AzureBlob ',
    N'  ); ',
    N' ',
    N' MERGE ',
    @tableName, 
    N' original',
    N' USING ',
    N' ( ',
    N'      SELECT * FROM OPENJSON(@json) WITH ',
    N'      (',
                STUFF(@schema, 1, 1, N''),   
    N'      ) modified ',
    N'      ON (original.col_href = modified.col_href)',
    N'      WHEN MATCHED ',
    N'          THEN UPDATE SET ',
                STUFF(@setCommand, 1, 1, N''),
    N'      WHEN NOT MATCHED BY TARGET THEN',
    N'      INSERT  ',
    N'      (',
                STUFF(@columns, 1, 1, N''),
    N'      )',
    N'      VALUES',
    N'      (',
    N'          SELECT * FROM OPENJSON(@json) ', 
    N'          WITH ', 
    N'          (',
                    STUFF(@schema, 1, 1, N''),   
    N'          ) ',
    N'      )',
    N' )'
);

PRINT @stm;

-- SET PARSEONLY off
-- EXEC sp_executesql @stm;

-- select * from claims


select top 100
    *
from accounts_receivables
where office_id = 'D18336'

select
    *
from claims
where office_id = 'D18336'
