
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                                CAST(BulkColumn AS NVARCHAR(MAX)) AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/V15543/reminders.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_patient_id varchar(8000),col_id varchar(8000),col_guarantor_id varchar(8000),col_appointment_sr_no varchar(8000),col_set_date varchar(8000),col_due_date varchar(8000),col_prior_date varchar(8000),col_provider_type varchar(8000),col_provider_id varchar(8000),col_time_unit varchar(8000),col_status varchar(8000),col_care_name varchar(8000),col_description varchar(8000),col_practice_id varchar(8000),col_practice varchar(8000),col_patient varchar(8000),col_guarantor varchar(8000),col_provider varchar(8000),col_appointment varchar(8000),col_interval_type varchar(8000),col_interval_length varchar(8000),col_procedure_code varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_patient_id],[col_id],[col_guarantor_id],[col_appointment_sr_no],[col_set_date],[col_due_date],[col_prior_date],[col_provider_type],[col_provider_id],[col_time_unit],[col_status],[col_care_name],[col_description],[col_practice_id],[col_practice],[col_patient],[col_guarantor],[col_provider],[col_appointment],[col_interval_type],[col_interval_length],[col_procedure_code],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-12T01:09:22.955Z',
                            '2020-05-12T01:09:22.955Z',
                            'V15543',
                            '' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_patient_id varchar(8000) '$.patient_id',col_id varchar(8000) '$.id',col_guarantor_id varchar(8000) '$.guarantor_id',col_appointment_sr_no varchar(8000) '$.appointment_sr_no',col_set_date varchar(8000) '$.set_date',col_due_date varchar(8000) '$.due_date',col_prior_date varchar(8000) '$.prior_date',col_provider_type varchar(8000) '$.provider_type',col_provider_id varchar(8000) '$.provider_id',col_time_unit varchar(8000) '$.time_unit',col_status varchar(8000) '$.status',col_care_name varchar(8000) '$.care_name',col_description varchar(8000) '$.description',col_practice_id varchar(8000) '$.practice_id',col_practice varchar(8000) '$.practice',col_patient varchar(8000) '$.patient',col_guarantor varchar(8000) '$.guarantor',col_provider varchar(8000) '$.provider',col_appointment varchar(8000) '$.appointment',col_interval_type varchar(8000) '$.interval_type',col_interval_length varchar(8000) '$.interval_length',col_procedure_code varchar(8000) '$.procedure_code'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE reminders original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_patient_id] = modified.[col_patient_id],original.[col_id] = modified.[col_id],original.[col_guarantor_id] = modified.[col_guarantor_id],original.[col_appointment_sr_no] = modified.[col_appointment_sr_no],original.[col_set_date] = modified.[col_set_date],original.[col_due_date] = modified.[col_due_date],original.[col_prior_date] = modified.[col_prior_date],original.[col_provider_type] = modified.[col_provider_type],original.[col_provider_id] = modified.[col_provider_id],original.[col_time_unit] = modified.[col_time_unit],original.[col_status] = modified.[col_status],original.[col_care_name] = modified.[col_care_name],original.[col_description] = modified.[col_description],original.[col_practice_id] = modified.[col_practice_id],original.[col_practice] = modified.[col_practice],original.[col_patient] = modified.[col_patient],original.[col_guarantor] = modified.[col_guarantor],original.[col_provider] = modified.[col_provider],original.[col_appointment] = modified.[col_appointment],original.[col_interval_type] = modified.[col_interval_type],original.[col_interval_length] = modified.[col_interval_length],original.[col_procedure_code] = modified.[col_procedure_code],
                        original.updated_at = '2020-05-12T01:09:22.955Z',
                        original.office_id = 'V15543',
                        practice_name = ''
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_patient_id],[col_id],[col_guarantor_id],[col_appointment_sr_no],[col_set_date],[col_due_date],[col_prior_date],[col_provider_type],[col_provider_id],[col_time_unit],[col_status],[col_care_name],[col_description],[col_practice_id],[col_practice],[col_patient],[col_guarantor],[col_provider],[col_appointment],[col_interval_type],[col_interval_length],[col_procedure_code],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_patient_id],modified.[col_id],modified.[col_guarantor_id],modified.[col_appointment_sr_no],modified.[col_set_date],modified.[col_due_date],modified.[col_prior_date],modified.[col_provider_type],modified.[col_provider_id],modified.[col_time_unit],modified.[col_status],modified.[col_care_name],modified.[col_description],modified.[col_practice_id],modified.[col_practice],modified.[col_patient],modified.[col_guarantor],modified.[col_provider],modified.[col_appointment],modified.[col_interval_type],modified.[col_interval_length],modified.[col_procedure_code],
                        '2020-05-12T01:09:22.955Z',
                        '2020-05-12T01:09:22.955Z',
                        'V15543',
                        ''
                        );
                    