
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                               '[' + CAST(BulkColumn AS NVARCHAR(MAX)) + ']' AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/streams/D34723/health_check.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_http_code varchar(8000),col_http_code_desc varchar(8000),col_allowed_methods varchar(8000),col_allow_origin varchar(8000),col_api_version varchar(8000),col_href varchar(8000),col_api_description varchar(8000),col_parameters varchar(8000),col_apis varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_http_code],[col_http_code_desc],[col_allowed_methods],[col_allow_origin],[col_api_version],[col_href],[col_api_description],[col_parameters],[col_apis],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-18T02:47:45.911Z',
                            '2020-05-18T02:47:45.911Z',
                            'D34723',
                            'Carolina Smiles' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_http_code varchar(8000) '$.http_code',col_http_code_desc varchar(8000) '$.http_code_desc',col_allowed_methods varchar(8000) '$.allowed_methods',col_allow_origin varchar(8000) '$.allow_origin',col_api_version varchar(8000) '$.api_version',col_href varchar(8000) '$.href',col_api_description varchar(8000) '$.api_description',col_parameters varchar(8000) '$.parameters',col_apis varchar(8000) '$.apis'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE health_check original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_http_code] = modified.[col_http_code],original.[col_http_code_desc] = modified.[col_http_code_desc],original.[col_allowed_methods] = modified.[col_allowed_methods],original.[col_allow_origin] = modified.[col_allow_origin],original.[col_api_version] = modified.[col_api_version],original.[col_href] = modified.[col_href],original.[col_api_description] = modified.[col_api_description],original.[col_parameters] = modified.[col_parameters],original.[col_apis] = modified.[col_apis],
                        original.updated_at = '2020-05-18T02:47:45.911Z',
                        original.office_id = 'D34723',
                        practice_name = 'Carolina Smiles'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_http_code],[col_http_code_desc],[col_allowed_methods],[col_allow_origin],[col_api_version],[col_href],[col_api_description],[col_parameters],[col_apis],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_http_code],modified.[col_http_code_desc],modified.[col_allowed_methods],modified.[col_allow_origin],modified.[col_api_version],modified.[col_href],modified.[col_api_description],modified.[col_parameters],modified.[col_apis],
                        '2020-05-18T02:47:45.911Z',
                        '2020-05-18T02:47:45.911Z',
                        'D34723',
                        'Carolina Smiles'
                        );
                    