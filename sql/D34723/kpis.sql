
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                               '[' + CAST(BulkColumn AS NVARCHAR(MAX)) + ']' AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/streams/D34723/kpis.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_category varchar(8000),col_key_performance_indicator varchar(8000),col_displayname varchar(8000),col_type varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_category],[col_key_performance_indicator],[col_displayname],[col_type],created_at, updated_at, office_id, practice_name)
                            SELECT *,
<<<<<<< HEAD
                            '2020-05-21T02:18:45.867Z',
                            '2020-05-21T02:18:45.867Z',
=======
                            '2020-05-18T04:49:40.722Z',
                            '2020-05-18T04:49:40.722Z',
>>>>>>> master
                            'D34723',
                            'Carolina Smiles' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_category varchar(8000) '$.category',col_key_performance_indicator varchar(8000) '$.key_performance_indicator',col_displayname varchar(8000) '$.displayname',col_type varchar(8000) '$.type'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE kpis original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_category] = modified.[col_category],original.[col_key_performance_indicator] = modified.[col_key_performance_indicator],original.[col_displayname] = modified.[col_displayname],original.[col_type] = modified.[col_type],
<<<<<<< HEAD
                        original.updated_at = '2020-05-21T02:18:45.867Z',
=======
                        original.updated_at = '2020-05-18T04:49:40.722Z',
>>>>>>> master
                        original.office_id = 'D34723',
                        practice_name = 'Carolina Smiles'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_category],[col_key_performance_indicator],[col_displayname],[col_type],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_category],modified.[col_key_performance_indicator],modified.[col_displayname],modified.[col_type],
<<<<<<< HEAD
                        '2020-05-21T02:18:45.867Z',
                        '2020-05-21T02:18:45.867Z',
=======
                        '2020-05-18T04:49:40.722Z',
                        '2020-05-18T04:49:40.722Z',
>>>>>>> master
                        'D34723',
                        'Carolina Smiles'
                        );
                    