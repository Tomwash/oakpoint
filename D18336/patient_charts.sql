
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                                CAST(BulkColumn AS NVARCHAR(MAX)) AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/D18336/patient_charts.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_patient_id varchar(8000),col_patient varchar(8000),col_practice_id varchar(8000),col_practice varchar(8000),col_tooth_number varchar(8000),col_surface varchar(8000),col_chart_date varchar(8000),col_description varchar(8000),col_type varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_patient_id],[col_patient],[col_practice_id],[col_practice],[col_tooth_number],[col_surface],[col_chart_date],[col_description],[col_type],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-12T01:04:12.298Z',
                            '2020-05-12T01:04:12.298Z',
                            'D18336',
                            'west cary dental' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_patient_id varchar(8000) '$.patient_id',col_patient varchar(8000) '$.patient',col_practice_id varchar(8000) '$.practice_id',col_practice varchar(8000) '$.practice',col_tooth_number varchar(8000) '$.tooth_number',col_surface varchar(8000) '$.surface',col_chart_date varchar(8000) '$.chart_date',col_description varchar(8000) '$.description',col_type varchar(8000) '$.type'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE patient_charts original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_patient_id] = modified.[col_patient_id],original.[col_patient] = modified.[col_patient],original.[col_practice_id] = modified.[col_practice_id],original.[col_practice] = modified.[col_practice],original.[col_tooth_number] = modified.[col_tooth_number],original.[col_surface] = modified.[col_surface],original.[col_chart_date] = modified.[col_chart_date],original.[col_description] = modified.[col_description],original.[col_type] = modified.[col_type],
                        original.updated_at = '2020-05-12T01:04:12.298Z',
                        original.office_id = 'D18336',
                        practice_name = 'west cary dental'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_patient_id],[col_patient],[col_practice_id],[col_practice],[col_tooth_number],[col_surface],[col_chart_date],[col_description],[col_type],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_patient_id],modified.[col_patient],modified.[col_practice_id],modified.[col_practice],modified.[col_tooth_number],modified.[col_surface],modified.[col_chart_date],modified.[col_description],modified.[col_type],
                        '2020-05-12T01:04:12.298Z',
                        '2020-05-12T01:04:12.298Z',
                        'D18336',
                        'west cary dental'
                        );
                    