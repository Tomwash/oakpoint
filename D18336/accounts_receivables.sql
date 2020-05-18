
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                               '[' + CAST(BulkColumn AS NVARCHAR(MAX)) + ']' AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/streams/D18336/accounts_receivables.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_http_code varchar(8000),col_http_code_desc varchar(8000),col_error_code varchar(8000),col_short_message varchar(8000),col_long_message varchar(8000),col_more_information varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_http_code],[col_http_code_desc],[col_error_code],[col_short_message],[col_long_message],[col_more_information],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-18T01:49:34.153Z',
                            '2020-05-18T01:49:34.153Z',
                            'D18336',
                            'west cary dental' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_http_code varchar(8000) '$.http_code',col_http_code_desc varchar(8000) '$.http_code_desc',col_error_code varchar(8000) '$.error_code',col_short_message varchar(8000) '$.short_message',col_long_message varchar(8000) '$.long_message',col_more_information varchar(8000) '$.more_information'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE accounts_receivables original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_http_code] = modified.[col_http_code],original.[col_http_code_desc] = modified.[col_http_code_desc],original.[col_error_code] = modified.[col_error_code],original.[col_short_message] = modified.[col_short_message],original.[col_long_message] = modified.[col_long_message],original.[col_more_information] = modified.[col_more_information],
                        original.updated_at = '2020-05-18T01:49:34.153Z',
                        original.office_id = 'D18336',
                        practice_name = 'west cary dental'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_http_code],[col_http_code_desc],[col_error_code],[col_short_message],[col_long_message],[col_more_information],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_http_code],modified.[col_http_code_desc],modified.[col_error_code],modified.[col_short_message],modified.[col_long_message],modified.[col_more_information],
                        '2020-05-18T01:49:34.153Z',
                        '2020-05-18T01:49:34.153Z',
                        'D18336',
                        'west cary dental'
                        );
                    
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                               '[' + CAST(BulkColumn AS NVARCHAR(MAX)) + ']' AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/streams/D18336/accounts_receivables.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_guarantor_id varchar(8000),col_amount_less_than_30 varchar(8000),col_amount_between_30_60 varchar(8000),col_amount_between_60_90 varchar(8000),col_amount_greater_than_90 varchar(8000),col_practice_id varchar(8000),col_entry_id varchar(8000),col_current_date varchar(8000),col_guarantor varchar(8000),col_practice varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_guarantor_id],[col_amount_less_than_30],[col_amount_between_30_60],[col_amount_between_60_90],[col_amount_greater_than_90],[col_practice_id],[col_entry_id],[col_current_date],[col_guarantor],[col_practice],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-18T02:21:02.144Z',
                            '2020-05-18T02:21:02.144Z',
                            'D18336',
                            'west cary dental' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_guarantor_id varchar(8000) '$.guarantor_id',col_amount_less_than_30 varchar(8000) '$.amount_less_than_30',col_amount_between_30_60 varchar(8000) '$.amount_between_30_60',col_amount_between_60_90 varchar(8000) '$.amount_between_60_90',col_amount_greater_than_90 varchar(8000) '$.amount_greater_than_90',col_practice_id varchar(8000) '$.practice_id',col_entry_id varchar(8000) '$.entry_id',col_current_date varchar(8000) '$.current_date',col_guarantor varchar(8000) '$.guarantor',col_practice varchar(8000) '$.practice'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE accounts_receivables original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_guarantor_id] = modified.[col_guarantor_id],original.[col_amount_less_than_30] = modified.[col_amount_less_than_30],original.[col_amount_between_30_60] = modified.[col_amount_between_30_60],original.[col_amount_between_60_90] = modified.[col_amount_between_60_90],original.[col_amount_greater_than_90] = modified.[col_amount_greater_than_90],original.[col_practice_id] = modified.[col_practice_id],original.[col_entry_id] = modified.[col_entry_id],original.[col_current_date] = modified.[col_current_date],original.[col_guarantor] = modified.[col_guarantor],original.[col_practice] = modified.[col_practice],
                        original.updated_at = '2020-05-18T02:21:02.144Z',
                        original.office_id = 'D18336',
                        practice_name = 'west cary dental'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_guarantor_id],[col_amount_less_than_30],[col_amount_between_30_60],[col_amount_between_60_90],[col_amount_greater_than_90],[col_practice_id],[col_entry_id],[col_current_date],[col_guarantor],[col_practice],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_guarantor_id],modified.[col_amount_less_than_30],modified.[col_amount_between_30_60],modified.[col_amount_between_60_90],modified.[col_amount_greater_than_90],modified.[col_practice_id],modified.[col_entry_id],modified.[col_current_date],modified.[col_guarantor],modified.[col_practice],
                        '2020-05-18T02:21:02.144Z',
                        '2020-05-18T02:21:02.144Z',
                        'D18336',
                        'west cary dental'
                        );
                    
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                               '[' + CAST(BulkColumn AS NVARCHAR(MAX)) + ']' AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/streams/D18336/accounts_receivables.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_guarantor_id varchar(8000),col_amount_less_than_30 varchar(8000),col_amount_between_30_60 varchar(8000),col_amount_between_60_90 varchar(8000),col_amount_greater_than_90 varchar(8000),col_practice_id varchar(8000),col_entry_id varchar(8000),col_current_date varchar(8000),col_guarantor varchar(8000),col_practice varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_guarantor_id],[col_amount_less_than_30],[col_amount_between_30_60],[col_amount_between_60_90],[col_amount_greater_than_90],[col_practice_id],[col_entry_id],[col_current_date],[col_guarantor],[col_practice],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-18T02:21:53.069Z',
                            '2020-05-18T02:21:53.069Z',
                            'D18336',
                            'west cary dental' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_guarantor_id varchar(8000) '$.guarantor_id',col_amount_less_than_30 varchar(8000) '$.amount_less_than_30',col_amount_between_30_60 varchar(8000) '$.amount_between_30_60',col_amount_between_60_90 varchar(8000) '$.amount_between_60_90',col_amount_greater_than_90 varchar(8000) '$.amount_greater_than_90',col_practice_id varchar(8000) '$.practice_id',col_entry_id varchar(8000) '$.entry_id',col_current_date varchar(8000) '$.current_date',col_guarantor varchar(8000) '$.guarantor',col_practice varchar(8000) '$.practice'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE accounts_receivables original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_guarantor_id] = modified.[col_guarantor_id],original.[col_amount_less_than_30] = modified.[col_amount_less_than_30],original.[col_amount_between_30_60] = modified.[col_amount_between_30_60],original.[col_amount_between_60_90] = modified.[col_amount_between_60_90],original.[col_amount_greater_than_90] = modified.[col_amount_greater_than_90],original.[col_practice_id] = modified.[col_practice_id],original.[col_entry_id] = modified.[col_entry_id],original.[col_current_date] = modified.[col_current_date],original.[col_guarantor] = modified.[col_guarantor],original.[col_practice] = modified.[col_practice],
                        original.updated_at = '2020-05-18T02:21:53.069Z',
                        original.office_id = 'D18336',
                        practice_name = 'west cary dental'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_guarantor_id],[col_amount_less_than_30],[col_amount_between_30_60],[col_amount_between_60_90],[col_amount_greater_than_90],[col_practice_id],[col_entry_id],[col_current_date],[col_guarantor],[col_practice],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_guarantor_id],modified.[col_amount_less_than_30],modified.[col_amount_between_30_60],modified.[col_amount_between_60_90],modified.[col_amount_greater_than_90],modified.[col_practice_id],modified.[col_entry_id],modified.[col_current_date],modified.[col_guarantor],modified.[col_practice],
                        '2020-05-18T02:21:53.069Z',
                        '2020-05-18T02:21:53.069Z',
                        'D18336',
                        'west cary dental'
                        );
                    
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                               '[' + CAST(BulkColumn AS NVARCHAR(MAX)) + ']' AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/streams/D18336/accounts_receivables.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_guarantor_id varchar(8000),col_amount_less_than_30 varchar(8000),col_amount_between_30_60 varchar(8000),col_amount_between_60_90 varchar(8000),col_amount_greater_than_90 varchar(8000),col_practice_id varchar(8000),col_entry_id varchar(8000),col_current_date varchar(8000),col_guarantor varchar(8000),col_practice varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_guarantor_id],[col_amount_less_than_30],[col_amount_between_30_60],[col_amount_between_60_90],[col_amount_greater_than_90],[col_practice_id],[col_entry_id],[col_current_date],[col_guarantor],[col_practice],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-18T02:24:38.809Z',
                            '2020-05-18T02:24:38.809Z',
                            'D18336',
                            'west cary dental' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_guarantor_id varchar(8000) '$.guarantor_id',col_amount_less_than_30 varchar(8000) '$.amount_less_than_30',col_amount_between_30_60 varchar(8000) '$.amount_between_30_60',col_amount_between_60_90 varchar(8000) '$.amount_between_60_90',col_amount_greater_than_90 varchar(8000) '$.amount_greater_than_90',col_practice_id varchar(8000) '$.practice_id',col_entry_id varchar(8000) '$.entry_id',col_current_date varchar(8000) '$.current_date',col_guarantor varchar(8000) '$.guarantor',col_practice varchar(8000) '$.practice'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE accounts_receivables original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_guarantor_id] = modified.[col_guarantor_id],original.[col_amount_less_than_30] = modified.[col_amount_less_than_30],original.[col_amount_between_30_60] = modified.[col_amount_between_30_60],original.[col_amount_between_60_90] = modified.[col_amount_between_60_90],original.[col_amount_greater_than_90] = modified.[col_amount_greater_than_90],original.[col_practice_id] = modified.[col_practice_id],original.[col_entry_id] = modified.[col_entry_id],original.[col_current_date] = modified.[col_current_date],original.[col_guarantor] = modified.[col_guarantor],original.[col_practice] = modified.[col_practice],
                        original.updated_at = '2020-05-18T02:24:38.809Z',
                        original.office_id = 'D18336',
                        practice_name = 'west cary dental'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_guarantor_id],[col_amount_less_than_30],[col_amount_between_30_60],[col_amount_between_60_90],[col_amount_greater_than_90],[col_practice_id],[col_entry_id],[col_current_date],[col_guarantor],[col_practice],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_guarantor_id],modified.[col_amount_less_than_30],modified.[col_amount_between_30_60],modified.[col_amount_between_60_90],modified.[col_amount_greater_than_90],modified.[col_practice_id],modified.[col_entry_id],modified.[col_current_date],modified.[col_guarantor],modified.[col_practice],
                        '2020-05-18T02:24:38.809Z',
                        '2020-05-18T02:24:38.809Z',
                        'D18336',
                        'west cary dental'
                        );
                    
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                               '[' + CAST(BulkColumn AS NVARCHAR(MAX)) + ']' AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/streams/D18336/accounts_receivables.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_guarantor_id varchar(8000),col_amount_less_than_30 varchar(8000),col_amount_between_30_60 varchar(8000),col_amount_between_60_90 varchar(8000),col_amount_greater_than_90 varchar(8000),col_practice_id varchar(8000),col_entry_id varchar(8000),col_current_date varchar(8000),col_guarantor varchar(8000),col_practice varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_guarantor_id],[col_amount_less_than_30],[col_amount_between_30_60],[col_amount_between_60_90],[col_amount_greater_than_90],[col_practice_id],[col_entry_id],[col_current_date],[col_guarantor],[col_practice],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-18T02:34:02.888Z',
                            '2020-05-18T02:34:02.888Z',
                            'D18336',
                            'west cary dental' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_guarantor_id varchar(8000) '$.guarantor_id',col_amount_less_than_30 varchar(8000) '$.amount_less_than_30',col_amount_between_30_60 varchar(8000) '$.amount_between_30_60',col_amount_between_60_90 varchar(8000) '$.amount_between_60_90',col_amount_greater_than_90 varchar(8000) '$.amount_greater_than_90',col_practice_id varchar(8000) '$.practice_id',col_entry_id varchar(8000) '$.entry_id',col_current_date varchar(8000) '$.current_date',col_guarantor varchar(8000) '$.guarantor',col_practice varchar(8000) '$.practice'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE accounts_receivables original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_guarantor_id] = modified.[col_guarantor_id],original.[col_amount_less_than_30] = modified.[col_amount_less_than_30],original.[col_amount_between_30_60] = modified.[col_amount_between_30_60],original.[col_amount_between_60_90] = modified.[col_amount_between_60_90],original.[col_amount_greater_than_90] = modified.[col_amount_greater_than_90],original.[col_practice_id] = modified.[col_practice_id],original.[col_entry_id] = modified.[col_entry_id],original.[col_current_date] = modified.[col_current_date],original.[col_guarantor] = modified.[col_guarantor],original.[col_practice] = modified.[col_practice],
                        original.updated_at = '2020-05-18T02:34:02.888Z',
                        original.office_id = 'D18336',
                        practice_name = 'west cary dental'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_guarantor_id],[col_amount_less_than_30],[col_amount_between_30_60],[col_amount_between_60_90],[col_amount_greater_than_90],[col_practice_id],[col_entry_id],[col_current_date],[col_guarantor],[col_practice],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_guarantor_id],modified.[col_amount_less_than_30],modified.[col_amount_between_30_60],modified.[col_amount_between_60_90],modified.[col_amount_greater_than_90],modified.[col_practice_id],modified.[col_entry_id],modified.[col_current_date],modified.[col_guarantor],modified.[col_practice],
                        '2020-05-18T02:34:02.888Z',
                        '2020-05-18T02:34:02.888Z',
                        'D18336',
                        'west cary dental'
                        );
                    