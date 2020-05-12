
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                                CAST(BulkColumn AS NVARCHAR(MAX)) AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/D18336/treatment_plans.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_plan_sr_no varchar(8000),col_patient_id varchar(8000),col_patient varchar(8000),col_guarantor_id varchar(8000),col_procedure_code varchar(8000),col_description varchar(8000),col_provider_id varchar(8000),col_provider varchar(8000),col_amount varchar(8000),col_entry_date varchar(8000),col_insurance_payment varchar(8000),col_treatment_plan_status varchar(8000),col_practice_id varchar(8000),col_practice varchar(8000),col_guarantor varchar(8000),col_procedure_date varchar(8000),col_tooth_from varchar(8000),col_tooth_to varchar(8000),col_surface varchar(8000),col_clinical_condition_id varchar(8000),col_collection_provider_id varchar(8000),col_created_by varchar(8000),col_appointment_sr_no varchar(8000),col_last_updated_by varchar(8000),col_primary_insurance_estimate varchar(8000),col_secondary_insurance_estimate varchar(8000),col_surface_quadrant_type varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_plan_sr_no],[col_patient_id],[col_patient],[col_guarantor_id],[col_procedure_code],[col_description],[col_provider_id],[col_provider],[col_amount],[col_entry_date],[col_insurance_payment],[col_treatment_plan_status],[col_practice_id],[col_practice],[col_guarantor],[col_procedure_date],[col_tooth_from],[col_tooth_to],[col_surface],[col_clinical_condition_id],[col_collection_provider_id],[col_created_by],[col_appointment_sr_no],[col_last_updated_by],[col_primary_insurance_estimate],[col_secondary_insurance_estimate],[col_surface_quadrant_type],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-12T01:04:32.827Z',
                            '2020-05-12T01:04:32.827Z',
                            'D18336',
                            'west cary dental' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_plan_sr_no varchar(8000) '$.plan_sr_no',col_patient_id varchar(8000) '$.patient_id',col_patient varchar(8000) '$.patient',col_guarantor_id varchar(8000) '$.guarantor_id',col_procedure_code varchar(8000) '$.procedure_code',col_description varchar(8000) '$.description',col_provider_id varchar(8000) '$.provider_id',col_provider varchar(8000) '$.provider',col_amount varchar(8000) '$.amount',col_entry_date varchar(8000) '$.entry_date',col_insurance_payment varchar(8000) '$.insurance_payment',col_treatment_plan_status varchar(8000) '$.treatment_plan_status',col_practice_id varchar(8000) '$.practice_id',col_practice varchar(8000) '$.practice',col_guarantor varchar(8000) '$.guarantor',col_procedure_date varchar(8000) '$.procedure_date',col_tooth_from varchar(8000) '$.tooth_from',col_tooth_to varchar(8000) '$.tooth_to',col_surface varchar(8000) '$.surface',col_clinical_condition_id varchar(8000) '$.clinical_condition_id',col_collection_provider_id varchar(8000) '$.collection_provider_id',col_created_by varchar(8000) '$.created_by',col_appointment_sr_no varchar(8000) '$.appointment_sr_no',col_last_updated_by varchar(8000) '$.last_updated_by',col_primary_insurance_estimate varchar(8000) '$.primary_insurance_estimate',col_secondary_insurance_estimate varchar(8000) '$.secondary_insurance_estimate',col_surface_quadrant_type varchar(8000) '$.surface_quadrant_type'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE treatment_plans original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_plan_sr_no] = modified.[col_plan_sr_no],original.[col_patient_id] = modified.[col_patient_id],original.[col_patient] = modified.[col_patient],original.[col_guarantor_id] = modified.[col_guarantor_id],original.[col_procedure_code] = modified.[col_procedure_code],original.[col_description] = modified.[col_description],original.[col_provider_id] = modified.[col_provider_id],original.[col_provider] = modified.[col_provider],original.[col_amount] = modified.[col_amount],original.[col_entry_date] = modified.[col_entry_date],original.[col_insurance_payment] = modified.[col_insurance_payment],original.[col_treatment_plan_status] = modified.[col_treatment_plan_status],original.[col_practice_id] = modified.[col_practice_id],original.[col_practice] = modified.[col_practice],original.[col_guarantor] = modified.[col_guarantor],original.[col_procedure_date] = modified.[col_procedure_date],original.[col_tooth_from] = modified.[col_tooth_from],original.[col_tooth_to] = modified.[col_tooth_to],original.[col_surface] = modified.[col_surface],original.[col_clinical_condition_id] = modified.[col_clinical_condition_id],original.[col_collection_provider_id] = modified.[col_collection_provider_id],original.[col_created_by] = modified.[col_created_by],original.[col_appointment_sr_no] = modified.[col_appointment_sr_no],original.[col_last_updated_by] = modified.[col_last_updated_by],original.[col_primary_insurance_estimate] = modified.[col_primary_insurance_estimate],original.[col_secondary_insurance_estimate] = modified.[col_secondary_insurance_estimate],original.[col_surface_quadrant_type] = modified.[col_surface_quadrant_type],
                        original.updated_at = '2020-05-12T01:04:32.827Z',
                        original.office_id = 'D18336',
                        practice_name = 'west cary dental'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_plan_sr_no],[col_patient_id],[col_patient],[col_guarantor_id],[col_procedure_code],[col_description],[col_provider_id],[col_provider],[col_amount],[col_entry_date],[col_insurance_payment],[col_treatment_plan_status],[col_practice_id],[col_practice],[col_guarantor],[col_procedure_date],[col_tooth_from],[col_tooth_to],[col_surface],[col_clinical_condition_id],[col_collection_provider_id],[col_created_by],[col_appointment_sr_no],[col_last_updated_by],[col_primary_insurance_estimate],[col_secondary_insurance_estimate],[col_surface_quadrant_type],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_plan_sr_no],modified.[col_patient_id],modified.[col_patient],modified.[col_guarantor_id],modified.[col_procedure_code],modified.[col_description],modified.[col_provider_id],modified.[col_provider],modified.[col_amount],modified.[col_entry_date],modified.[col_insurance_payment],modified.[col_treatment_plan_status],modified.[col_practice_id],modified.[col_practice],modified.[col_guarantor],modified.[col_procedure_date],modified.[col_tooth_from],modified.[col_tooth_to],modified.[col_surface],modified.[col_clinical_condition_id],modified.[col_collection_provider_id],modified.[col_created_by],modified.[col_appointment_sr_no],modified.[col_last_updated_by],modified.[col_primary_insurance_estimate],modified.[col_secondary_insurance_estimate],modified.[col_surface_quadrant_type],
                        '2020-05-12T01:04:32.827Z',
                        '2020-05-12T01:04:32.827Z',
                        'D18336',
                        'west cary dental'
                        );
                    