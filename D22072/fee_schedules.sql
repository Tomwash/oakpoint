
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                                CAST(BulkColumn AS NVARCHAR(MAX)) AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/D22072/fee_schedules.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_procedure_code varchar(8000),col_fee_no varchar(8000),col_fee_name varchar(8000),col_fee_amount varchar(8000),col_practice_id varchar(8000),col_practice varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_procedure_code],[col_fee_no],[col_fee_name],[col_fee_amount],[col_practice_id],[col_practice],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-12T01:04:44.338Z',
                            '2020-05-12T01:04:44.338Z',
                            'D22072',
                            '' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_procedure_code varchar(8000) '$.procedure_code',col_fee_no varchar(8000) '$.fee_no',col_fee_name varchar(8000) '$.fee_name',col_fee_amount varchar(8000) '$.fee_amount',col_practice_id varchar(8000) '$.practice_id',col_practice varchar(8000) '$.practice'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE fee_schedules original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_procedure_code] = modified.[col_procedure_code],original.[col_fee_no] = modified.[col_fee_no],original.[col_fee_name] = modified.[col_fee_name],original.[col_fee_amount] = modified.[col_fee_amount],original.[col_practice_id] = modified.[col_practice_id],original.[col_practice] = modified.[col_practice],
                        original.updated_at = '2020-05-12T01:04:44.338Z',
                        original.office_id = 'D22072',
                        practice_name = ''
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_procedure_code],[col_fee_no],[col_fee_name],[col_fee_amount],[col_practice_id],[col_practice],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_procedure_code],modified.[col_fee_no],modified.[col_fee_name],modified.[col_fee_amount],modified.[col_practice_id],modified.[col_practice],
                        '2020-05-12T01:04:44.338Z',
                        '2020-05-12T01:04:44.338Z',
                        'D22072',
                        ''
                        );
                    