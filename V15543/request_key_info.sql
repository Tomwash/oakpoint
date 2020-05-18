
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                               '[' + CAST(BulkColumn AS NVARCHAR(MAX)) + ']' AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/streams/V15543/request_key_info.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_request_key varchar(8000),col_start_time varchar(8000),col_end_time varchar(8000),col_expires_in varchar(8000),col_issued_to varchar(8000),col_status varchar(8000),col_request_count varchar(8000),col_scope varchar(8000),col_domain varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_request_key],[col_start_time],[col_end_time],[col_expires_in],[col_issued_to],[col_status],[col_request_count],[col_scope],[col_domain],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-18T02:50:21.532Z',
                            '2020-05-18T02:50:21.532Z',
                            'V15543',
                            '' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_request_key varchar(8000) '$.request_key',col_start_time varchar(8000) '$.start_time',col_end_time varchar(8000) '$.end_time',col_expires_in varchar(8000) '$.expires_in',col_issued_to varchar(8000) '$.issued_to',col_status varchar(8000) '$.status',col_request_count varchar(8000) '$.request_count',col_scope varchar(8000) '$.scope',col_domain varchar(8000) '$.domain'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE request_key_info original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_request_key] = modified.[col_request_key],original.[col_start_time] = modified.[col_start_time],original.[col_end_time] = modified.[col_end_time],original.[col_expires_in] = modified.[col_expires_in],original.[col_issued_to] = modified.[col_issued_to],original.[col_status] = modified.[col_status],original.[col_request_count] = modified.[col_request_count],original.[col_scope] = modified.[col_scope],original.[col_domain] = modified.[col_domain],
                        original.updated_at = '2020-05-18T02:50:21.532Z',
                        original.office_id = 'V15543',
                        practice_name = ''
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_request_key],[col_start_time],[col_end_time],[col_expires_in],[col_issued_to],[col_status],[col_request_count],[col_scope],[col_domain],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_request_key],modified.[col_start_time],modified.[col_end_time],modified.[col_expires_in],modified.[col_issued_to],modified.[col_status],modified.[col_request_count],modified.[col_scope],modified.[col_domain],
                        '2020-05-18T02:50:21.532Z',
                        '2020-05-18T02:50:21.532Z',
                        'V15543',
                        ''
                        );
                    