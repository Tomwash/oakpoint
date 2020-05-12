
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                                CAST(BulkColumn AS NVARCHAR(MAX)) AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/D24510/procedure_codes.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_procedure_code varchar(8000),col_procedure_code_description varchar(8000),col_abbreaviation varchar(8000),col_procedure_code_category varchar(8000),col_practice_id varchar(8000),col_procedure_code_category_id varchar(8000),col_practice varchar(8000),col_explosion_code varchar(8000),col_submit_to_insurance varchar(8000),col_allow_discount varchar(8000),col_procedure_code_effect_on_patient_balance varchar(8000),col_procedure_code_effect_on_provider_production varchar(8000),col_procedure_code_effect_on_provider_collection varchar(8000),col_procedure_code_type varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_procedure_code],[col_procedure_code_description],[col_abbreaviation],[col_procedure_code_category],[col_practice_id],[col_procedure_code_category_id],[col_practice],[col_explosion_code],[col_submit_to_insurance],[col_allow_discount],[col_procedure_code_effect_on_patient_balance],[col_procedure_code_effect_on_provider_production],[col_procedure_code_effect_on_provider_collection],[col_procedure_code_type],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-12T01:06:40.402Z',
                            '2020-05-12T01:06:40.402Z',
                            'D24510',
                            'Advanced Dental Center of Florence' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_procedure_code varchar(8000) '$.procedure_code',col_procedure_code_description varchar(8000) '$.procedure_code_description',col_abbreaviation varchar(8000) '$.abbreaviation',col_procedure_code_category varchar(8000) '$.procedure_code_category',col_practice_id varchar(8000) '$.practice_id',col_procedure_code_category_id varchar(8000) '$.procedure_code_category_id',col_practice varchar(8000) '$.practice',col_explosion_code varchar(8000) '$.explosion_code',col_submit_to_insurance varchar(8000) '$.submit_to_insurance',col_allow_discount varchar(8000) '$.allow_discount',col_procedure_code_effect_on_patient_balance varchar(8000) '$.procedure_code_effect_on_patient_balance',col_procedure_code_effect_on_provider_production varchar(8000) '$.procedure_code_effect_on_provider_production',col_procedure_code_effect_on_provider_collection varchar(8000) '$.procedure_code_effect_on_provider_collection',col_procedure_code_type varchar(8000) '$.procedure_code_type'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE procedure_codes original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_procedure_code] = modified.[col_procedure_code],original.[col_procedure_code_description] = modified.[col_procedure_code_description],original.[col_abbreaviation] = modified.[col_abbreaviation],original.[col_procedure_code_category] = modified.[col_procedure_code_category],original.[col_practice_id] = modified.[col_practice_id],original.[col_procedure_code_category_id] = modified.[col_procedure_code_category_id],original.[col_practice] = modified.[col_practice],original.[col_explosion_code] = modified.[col_explosion_code],original.[col_submit_to_insurance] = modified.[col_submit_to_insurance],original.[col_allow_discount] = modified.[col_allow_discount],original.[col_procedure_code_effect_on_patient_balance] = modified.[col_procedure_code_effect_on_patient_balance],original.[col_procedure_code_effect_on_provider_production] = modified.[col_procedure_code_effect_on_provider_production],original.[col_procedure_code_effect_on_provider_collection] = modified.[col_procedure_code_effect_on_provider_collection],original.[col_procedure_code_type] = modified.[col_procedure_code_type],
                        original.updated_at = '2020-05-12T01:06:40.402Z',
                        original.office_id = 'D24510',
                        practice_name = 'Advanced Dental Center of Florence'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_procedure_code],[col_procedure_code_description],[col_abbreaviation],[col_procedure_code_category],[col_practice_id],[col_procedure_code_category_id],[col_practice],[col_explosion_code],[col_submit_to_insurance],[col_allow_discount],[col_procedure_code_effect_on_patient_balance],[col_procedure_code_effect_on_provider_production],[col_procedure_code_effect_on_provider_collection],[col_procedure_code_type],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_procedure_code],modified.[col_procedure_code_description],modified.[col_abbreaviation],modified.[col_procedure_code_category],modified.[col_practice_id],modified.[col_procedure_code_category_id],modified.[col_practice],modified.[col_explosion_code],modified.[col_submit_to_insurance],modified.[col_allow_discount],modified.[col_procedure_code_effect_on_patient_balance],modified.[col_procedure_code_effect_on_provider_production],modified.[col_procedure_code_effect_on_provider_collection],modified.[col_procedure_code_type],
                        '2020-05-12T01:06:40.402Z',
                        '2020-05-12T01:06:40.402Z',
                        'D24510',
                        'Advanced Dental Center of Florence'
                        );
                    