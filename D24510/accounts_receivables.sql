
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                                CAST(BulkColumn AS NVARCHAR(MAX)) AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/D24510/accounts_receivables.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
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
                            '2020-05-12T01:05:46.750Z',
                            '2020-05-12T01:05:46.750Z',
                            'D24510',
                            'Advanced Dental Center of Florence' FROM OPENJSON(@json)  
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
                        original.updated_at = '2020-05-12T01:05:46.750Z',
                        original.office_id = 'D24510',
                        practice_name = 'Advanced Dental Center of Florence'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_guarantor_id],[col_amount_less_than_30],[col_amount_between_30_60],[col_amount_between_60_90],[col_amount_greater_than_90],[col_practice_id],[col_entry_id],[col_current_date],[col_guarantor],[col_practice],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_guarantor_id],modified.[col_amount_less_than_30],modified.[col_amount_between_30_60],modified.[col_amount_between_60_90],modified.[col_amount_greater_than_90],modified.[col_practice_id],modified.[col_entry_id],modified.[col_current_date],modified.[col_guarantor],modified.[col_practice],
                        '2020-05-12T01:05:46.750Z',
                        '2020-05-12T01:05:46.750Z',
                        'D24510',
                        'Advanced Dental Center of Florence'
                        );
                    