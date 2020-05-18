
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                               '[' + CAST(BulkColumn AS NVARCHAR(MAX)) + ']' AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/streams/D18336/patient_conditions.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_patient_condition_id varchar(8000),col_patient_id varchar(8000),col_entered_date varchar(8000),col_description varchar(8000),col_surface varchar(8000),col_provider_id varchar(8000),col_tooth varchar(8000),col_no_longer_present varchar(8000),col_practice_id varchar(8000),col_patient varchar(8000),col_practice varchar(8000),col_provider varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_patient_condition_id],[col_patient_id],[col_entered_date],[col_description],[col_surface],[col_provider_id],[col_tooth],[col_no_longer_present],[col_practice_id],[col_patient],[col_practice],[col_provider],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-18T02:22:21.213Z',
                            '2020-05-18T02:22:21.213Z',
                            'D18336',
                            'west cary dental' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_patient_condition_id varchar(8000) '$.patient_condition_id',col_patient_id varchar(8000) '$.patient_id',col_entered_date varchar(8000) '$.entered_date',col_description varchar(8000) '$.description',col_surface varchar(8000) '$.surface',col_provider_id varchar(8000) '$.provider_id',col_tooth varchar(8000) '$.tooth',col_no_longer_present varchar(8000) '$.no_longer_present',col_practice_id varchar(8000) '$.practice_id',col_patient varchar(8000) '$.patient',col_practice varchar(8000) '$.practice',col_provider varchar(8000) '$.provider'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE patient_conditions original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_patient_condition_id] = modified.[col_patient_condition_id],original.[col_patient_id] = modified.[col_patient_id],original.[col_entered_date] = modified.[col_entered_date],original.[col_description] = modified.[col_description],original.[col_surface] = modified.[col_surface],original.[col_provider_id] = modified.[col_provider_id],original.[col_tooth] = modified.[col_tooth],original.[col_no_longer_present] = modified.[col_no_longer_present],original.[col_practice_id] = modified.[col_practice_id],original.[col_patient] = modified.[col_patient],original.[col_practice] = modified.[col_practice],original.[col_provider] = modified.[col_provider],
                        original.updated_at = '2020-05-18T02:22:21.213Z',
                        original.office_id = 'D18336',
                        practice_name = 'west cary dental'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_patient_condition_id],[col_patient_id],[col_entered_date],[col_description],[col_surface],[col_provider_id],[col_tooth],[col_no_longer_present],[col_practice_id],[col_patient],[col_practice],[col_provider],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_patient_condition_id],modified.[col_patient_id],modified.[col_entered_date],modified.[col_description],modified.[col_surface],modified.[col_provider_id],modified.[col_tooth],modified.[col_no_longer_present],modified.[col_practice_id],modified.[col_patient],modified.[col_practice],modified.[col_provider],
                        '2020-05-18T02:22:21.213Z',
                        '2020-05-18T02:22:21.213Z',
                        'D18336',
                        'west cary dental'
                        );
                    
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                               '[' + CAST(BulkColumn AS NVARCHAR(MAX)) + ']' AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/streams/D18336/patient_conditions.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_patient_condition_id varchar(8000),col_patient_id varchar(8000),col_entered_date varchar(8000),col_description varchar(8000),col_surface varchar(8000),col_provider_id varchar(8000),col_tooth varchar(8000),col_no_longer_present varchar(8000),col_practice_id varchar(8000),col_patient varchar(8000),col_practice varchar(8000),col_provider varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_patient_condition_id],[col_patient_id],[col_entered_date],[col_description],[col_surface],[col_provider_id],[col_tooth],[col_no_longer_present],[col_practice_id],[col_patient],[col_practice],[col_provider],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-18T02:25:10.169Z',
                            '2020-05-18T02:25:10.169Z',
                            'D18336',
                            'west cary dental' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_patient_condition_id varchar(8000) '$.patient_condition_id',col_patient_id varchar(8000) '$.patient_id',col_entered_date varchar(8000) '$.entered_date',col_description varchar(8000) '$.description',col_surface varchar(8000) '$.surface',col_provider_id varchar(8000) '$.provider_id',col_tooth varchar(8000) '$.tooth',col_no_longer_present varchar(8000) '$.no_longer_present',col_practice_id varchar(8000) '$.practice_id',col_patient varchar(8000) '$.patient',col_practice varchar(8000) '$.practice',col_provider varchar(8000) '$.provider'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE patient_conditions original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_patient_condition_id] = modified.[col_patient_condition_id],original.[col_patient_id] = modified.[col_patient_id],original.[col_entered_date] = modified.[col_entered_date],original.[col_description] = modified.[col_description],original.[col_surface] = modified.[col_surface],original.[col_provider_id] = modified.[col_provider_id],original.[col_tooth] = modified.[col_tooth],original.[col_no_longer_present] = modified.[col_no_longer_present],original.[col_practice_id] = modified.[col_practice_id],original.[col_patient] = modified.[col_patient],original.[col_practice] = modified.[col_practice],original.[col_provider] = modified.[col_provider],
                        original.updated_at = '2020-05-18T02:25:10.169Z',
                        original.office_id = 'D18336',
                        practice_name = 'west cary dental'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_patient_condition_id],[col_patient_id],[col_entered_date],[col_description],[col_surface],[col_provider_id],[col_tooth],[col_no_longer_present],[col_practice_id],[col_patient],[col_practice],[col_provider],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_patient_condition_id],modified.[col_patient_id],modified.[col_entered_date],modified.[col_description],modified.[col_surface],modified.[col_provider_id],modified.[col_tooth],modified.[col_no_longer_present],modified.[col_practice_id],modified.[col_patient],modified.[col_practice],modified.[col_provider],
                        '2020-05-18T02:25:10.169Z',
                        '2020-05-18T02:25:10.169Z',
                        'D18336',
                        'west cary dental'
                        );
                    
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                               '[' + CAST(BulkColumn AS NVARCHAR(MAX)) + ']' AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/streams/D18336/patient_conditions.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_patient_condition_id varchar(8000),col_patient_id varchar(8000),col_entered_date varchar(8000),col_description varchar(8000),col_surface varchar(8000),col_provider_id varchar(8000),col_tooth varchar(8000),col_no_longer_present varchar(8000),col_practice_id varchar(8000),col_patient varchar(8000),col_practice varchar(8000),col_provider varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_patient_condition_id],[col_patient_id],[col_entered_date],[col_description],[col_surface],[col_provider_id],[col_tooth],[col_no_longer_present],[col_practice_id],[col_patient],[col_practice],[col_provider],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-18T02:34:23.938Z',
                            '2020-05-18T02:34:23.938Z',
                            'D18336',
                            'west cary dental' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_patient_condition_id varchar(8000) '$.patient_condition_id',col_patient_id varchar(8000) '$.patient_id',col_entered_date varchar(8000) '$.entered_date',col_description varchar(8000) '$.description',col_surface varchar(8000) '$.surface',col_provider_id varchar(8000) '$.provider_id',col_tooth varchar(8000) '$.tooth',col_no_longer_present varchar(8000) '$.no_longer_present',col_practice_id varchar(8000) '$.practice_id',col_patient varchar(8000) '$.patient',col_practice varchar(8000) '$.practice',col_provider varchar(8000) '$.provider'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE patient_conditions original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_patient_condition_id] = modified.[col_patient_condition_id],original.[col_patient_id] = modified.[col_patient_id],original.[col_entered_date] = modified.[col_entered_date],original.[col_description] = modified.[col_description],original.[col_surface] = modified.[col_surface],original.[col_provider_id] = modified.[col_provider_id],original.[col_tooth] = modified.[col_tooth],original.[col_no_longer_present] = modified.[col_no_longer_present],original.[col_practice_id] = modified.[col_practice_id],original.[col_patient] = modified.[col_patient],original.[col_practice] = modified.[col_practice],original.[col_provider] = modified.[col_provider],
                        original.updated_at = '2020-05-18T02:34:23.938Z',
                        original.office_id = 'D18336',
                        practice_name = 'west cary dental'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_patient_condition_id],[col_patient_id],[col_entered_date],[col_description],[col_surface],[col_provider_id],[col_tooth],[col_no_longer_present],[col_practice_id],[col_patient],[col_practice],[col_provider],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_patient_condition_id],modified.[col_patient_id],modified.[col_entered_date],modified.[col_description],modified.[col_surface],modified.[col_provider_id],modified.[col_tooth],modified.[col_no_longer_present],modified.[col_practice_id],modified.[col_patient],modified.[col_practice],modified.[col_provider],
                        '2020-05-18T02:34:23.938Z',
                        '2020-05-18T02:34:23.938Z',
                        'D18336',
                        'west cary dental'
                        );
                    