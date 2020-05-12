
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                                CAST(BulkColumn AS NVARCHAR(MAX)) AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/O22046/practice_variables.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_service_name varchar(8000),col_value varchar(8000),col_description varchar(8000),col_practice_id varchar(8000),col_practice varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_service_name],[col_value],[col_description],[col_practice_id],[col_practice],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-12T01:08:41.584Z',
                            '2020-05-12T01:08:41.584Z',
                            'O22046',
                            'Carolina Braces' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_service_name varchar(8000) '$.service_name',col_value varchar(8000) '$.value',col_description varchar(8000) '$.description',col_practice_id varchar(8000) '$.practice_id',col_practice varchar(8000) '$.practice'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE practice_variables original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_service_name] = modified.[col_service_name],original.[col_value] = modified.[col_value],original.[col_description] = modified.[col_description],original.[col_practice_id] = modified.[col_practice_id],original.[col_practice] = modified.[col_practice],
                        original.updated_at = '2020-05-12T01:08:41.584Z',
                        original.office_id = 'O22046',
                        practice_name = 'Carolina Braces'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_service_name],[col_value],[col_description],[col_practice_id],[col_practice],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_service_name],modified.[col_value],modified.[col_description],modified.[col_practice_id],modified.[col_practice],
                        '2020-05-12T01:08:41.584Z',
                        '2020-05-12T01:08:41.584Z',
                        'O22046',
                        'Carolina Braces'
                        );
                    