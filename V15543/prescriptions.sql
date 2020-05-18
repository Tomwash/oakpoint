
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                                CAST(BulkColumn AS NVARCHAR(MAX)) AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/V15543/prescriptions.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_prescription_id varchar(8000),col_guarantor_id varchar(8000),col_guarantor varchar(8000),col_patient_id varchar(8000),col_patient varchar(8000),col_staff varchar(8000),col_provider_id varchar(8000),col_provider varchar(8000),col_item_type varchar(8000),col_procedure_code_id varchar(8000),col_procedure_code varchar(8000),col_direction varchar(8000),col_contradiction varchar(8000),col_quantity varchar(8000),col_refills varchar(8000),col_manufacturer varchar(8000),col_expiry_date varchar(8000),col_copies varchar(8000),col_refill_date varchar(8000),col_sig_code varchar(8000),col_description varchar(8000),col_amount varchar(8000),col_practice_id varchar(8000),col_practice varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_prescription_id],[col_guarantor_id],[col_guarantor],[col_patient_id],[col_patient],[col_staff],[col_provider_id],[col_provider],[col_item_type],[col_procedure_code_id],[col_procedure_code],[col_direction],[col_contradiction],[col_quantity],[col_refills],[col_manufacturer],[col_expiry_date],[col_copies],[col_refill_date],[col_sig_code],[col_description],[col_amount],[col_practice_id],[col_practice],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-12T01:09:11.456Z',
                            '2020-05-12T01:09:11.456Z',
                            'V15543',
                            '' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_prescription_id varchar(8000) '$.prescription_id',col_guarantor_id varchar(8000) '$.guarantor_id',col_guarantor varchar(8000) '$.guarantor',col_patient_id varchar(8000) '$.patient_id',col_patient varchar(8000) '$.patient',col_staff varchar(8000) '$.staff',col_provider_id varchar(8000) '$.provider_id',col_provider varchar(8000) '$.provider',col_item_type varchar(8000) '$.item_type',col_procedure_code_id varchar(8000) '$.procedure_code_id',col_procedure_code varchar(8000) '$.procedure_code',col_direction varchar(8000) '$.direction',col_contradiction varchar(8000) '$.contradiction',col_quantity varchar(8000) '$.quantity',col_refills varchar(8000) '$.refills',col_manufacturer varchar(8000) '$.manufacturer',col_expiry_date varchar(8000) '$.expiry_date',col_copies varchar(8000) '$.copies',col_refill_date varchar(8000) '$.refill_date',col_sig_code varchar(8000) '$.sig_code',col_description varchar(8000) '$.description',col_amount varchar(8000) '$.amount',col_practice_id varchar(8000) '$.practice_id',col_practice varchar(8000) '$.practice'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE prescriptions original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_prescription_id] = modified.[col_prescription_id],original.[col_guarantor_id] = modified.[col_guarantor_id],original.[col_guarantor] = modified.[col_guarantor],original.[col_patient_id] = modified.[col_patient_id],original.[col_patient] = modified.[col_patient],original.[col_staff] = modified.[col_staff],original.[col_provider_id] = modified.[col_provider_id],original.[col_provider] = modified.[col_provider],original.[col_item_type] = modified.[col_item_type],original.[col_procedure_code_id] = modified.[col_procedure_code_id],original.[col_procedure_code] = modified.[col_procedure_code],original.[col_direction] = modified.[col_direction],original.[col_contradiction] = modified.[col_contradiction],original.[col_quantity] = modified.[col_quantity],original.[col_refills] = modified.[col_refills],original.[col_manufacturer] = modified.[col_manufacturer],original.[col_expiry_date] = modified.[col_expiry_date],original.[col_copies] = modified.[col_copies],original.[col_refill_date] = modified.[col_refill_date],original.[col_sig_code] = modified.[col_sig_code],original.[col_description] = modified.[col_description],original.[col_amount] = modified.[col_amount],original.[col_practice_id] = modified.[col_practice_id],original.[col_practice] = modified.[col_practice],
                        original.updated_at = '2020-05-12T01:09:11.456Z',
                        original.office_id = 'V15543',
                        practice_name = ''
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_prescription_id],[col_guarantor_id],[col_guarantor],[col_patient_id],[col_patient],[col_staff],[col_provider_id],[col_provider],[col_item_type],[col_procedure_code_id],[col_procedure_code],[col_direction],[col_contradiction],[col_quantity],[col_refills],[col_manufacturer],[col_expiry_date],[col_copies],[col_refill_date],[col_sig_code],[col_description],[col_amount],[col_practice_id],[col_practice],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_prescription_id],modified.[col_guarantor_id],modified.[col_guarantor],modified.[col_patient_id],modified.[col_patient],modified.[col_staff],modified.[col_provider_id],modified.[col_provider],modified.[col_item_type],modified.[col_procedure_code_id],modified.[col_procedure_code],modified.[col_direction],modified.[col_contradiction],modified.[col_quantity],modified.[col_refills],modified.[col_manufacturer],modified.[col_expiry_date],modified.[col_copies],modified.[col_refill_date],modified.[col_sig_code],modified.[col_description],modified.[col_amount],modified.[col_practice_id],modified.[col_practice],
                        '2020-05-12T01:09:11.456Z',
                        '2020-05-12T01:09:11.456Z',
                        'V15543',
                        ''
                        );
                    
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                               '[' + CAST(BulkColumn AS NVARCHAR(MAX)) + ']' AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/streams/V15543/prescriptions.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_prescription_id varchar(8000),col_guarantor_id varchar(8000),col_guarantor varchar(8000),col_patient_id varchar(8000),col_patient varchar(8000),col_staff varchar(8000),col_provider_id varchar(8000),col_provider varchar(8000),col_item_type varchar(8000),col_procedure_code_id varchar(8000),col_procedure_code varchar(8000),col_direction varchar(8000),col_contradiction varchar(8000),col_quantity varchar(8000),col_refills varchar(8000),col_manufacturer varchar(8000),col_expiry_date varchar(8000),col_copies varchar(8000),col_refill_date varchar(8000),col_sig_code varchar(8000),col_description varchar(8000),col_amount varchar(8000),col_practice_id varchar(8000),col_practice varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_prescription_id],[col_guarantor_id],[col_guarantor],[col_patient_id],[col_patient],[col_staff],[col_provider_id],[col_provider],[col_item_type],[col_procedure_code_id],[col_procedure_code],[col_direction],[col_contradiction],[col_quantity],[col_refills],[col_manufacturer],[col_expiry_date],[col_copies],[col_refill_date],[col_sig_code],[col_description],[col_amount],[col_practice_id],[col_practice],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-18T02:50:15.556Z',
                            '2020-05-18T02:50:15.556Z',
                            'V15543',
                            '' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_prescription_id varchar(8000) '$.prescription_id',col_guarantor_id varchar(8000) '$.guarantor_id',col_guarantor varchar(8000) '$.guarantor',col_patient_id varchar(8000) '$.patient_id',col_patient varchar(8000) '$.patient',col_staff varchar(8000) '$.staff',col_provider_id varchar(8000) '$.provider_id',col_provider varchar(8000) '$.provider',col_item_type varchar(8000) '$.item_type',col_procedure_code_id varchar(8000) '$.procedure_code_id',col_procedure_code varchar(8000) '$.procedure_code',col_direction varchar(8000) '$.direction',col_contradiction varchar(8000) '$.contradiction',col_quantity varchar(8000) '$.quantity',col_refills varchar(8000) '$.refills',col_manufacturer varchar(8000) '$.manufacturer',col_expiry_date varchar(8000) '$.expiry_date',col_copies varchar(8000) '$.copies',col_refill_date varchar(8000) '$.refill_date',col_sig_code varchar(8000) '$.sig_code',col_description varchar(8000) '$.description',col_amount varchar(8000) '$.amount',col_practice_id varchar(8000) '$.practice_id',col_practice varchar(8000) '$.practice'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE prescriptions original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_prescription_id] = modified.[col_prescription_id],original.[col_guarantor_id] = modified.[col_guarantor_id],original.[col_guarantor] = modified.[col_guarantor],original.[col_patient_id] = modified.[col_patient_id],original.[col_patient] = modified.[col_patient],original.[col_staff] = modified.[col_staff],original.[col_provider_id] = modified.[col_provider_id],original.[col_provider] = modified.[col_provider],original.[col_item_type] = modified.[col_item_type],original.[col_procedure_code_id] = modified.[col_procedure_code_id],original.[col_procedure_code] = modified.[col_procedure_code],original.[col_direction] = modified.[col_direction],original.[col_contradiction] = modified.[col_contradiction],original.[col_quantity] = modified.[col_quantity],original.[col_refills] = modified.[col_refills],original.[col_manufacturer] = modified.[col_manufacturer],original.[col_expiry_date] = modified.[col_expiry_date],original.[col_copies] = modified.[col_copies],original.[col_refill_date] = modified.[col_refill_date],original.[col_sig_code] = modified.[col_sig_code],original.[col_description] = modified.[col_description],original.[col_amount] = modified.[col_amount],original.[col_practice_id] = modified.[col_practice_id],original.[col_practice] = modified.[col_practice],
                        original.updated_at = '2020-05-18T02:50:15.556Z',
                        original.office_id = 'V15543',
                        practice_name = ''
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_prescription_id],[col_guarantor_id],[col_guarantor],[col_patient_id],[col_patient],[col_staff],[col_provider_id],[col_provider],[col_item_type],[col_procedure_code_id],[col_procedure_code],[col_direction],[col_contradiction],[col_quantity],[col_refills],[col_manufacturer],[col_expiry_date],[col_copies],[col_refill_date],[col_sig_code],[col_description],[col_amount],[col_practice_id],[col_practice],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_prescription_id],modified.[col_guarantor_id],modified.[col_guarantor],modified.[col_patient_id],modified.[col_patient],modified.[col_staff],modified.[col_provider_id],modified.[col_provider],modified.[col_item_type],modified.[col_procedure_code_id],modified.[col_procedure_code],modified.[col_direction],modified.[col_contradiction],modified.[col_quantity],modified.[col_refills],modified.[col_manufacturer],modified.[col_expiry_date],modified.[col_copies],modified.[col_refill_date],modified.[col_sig_code],modified.[col_description],modified.[col_amount],modified.[col_practice_id],modified.[col_practice],
                        '2020-05-18T02:50:15.556Z',
                        '2020-05-18T02:50:15.556Z',
                        'V15543',
                        ''
                        );
                    