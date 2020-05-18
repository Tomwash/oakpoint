
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                               '[' + CAST(BulkColumn AS NVARCHAR(MAX)) + ']' AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/streams/V15543/transactions.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_transaction_sr_no varchar(8000),col_patient_id varchar(8000),col_patient varchar(8000),col_guarantor_id varchar(8000),col_guarantor varchar(8000),col_transaction_type varchar(8000),col_transaction_date varchar(8000),col_transaction_entry_date varchar(8000),col_procedure_code varchar(8000),col_procedure_description varchar(8000),col_provider_id varchar(8000),col_provider varchar(8000),col_amount varchar(8000),col_tooth_from varchar(8000),col_tooth_to varchar(8000),col_estimated_insurance_payment varchar(8000),col_practice_id varchar(8000),col_practice varchar(8000),col_invoice_number varchar(8000),col_invoice_status varchar(8000),col_invoice_item varchar(8000),col_quantity varchar(8000),col_claim_sr_no varchar(8000),col_claim varchar(8000),col_pre_authorization_id varchar(8000),col_payment_type varchar(8000),col_surface varchar(8000),col_note varchar(8000),col_is_group_transaction varchar(8000),col_cheque_no varchar(8000),col_clinical_condition_id varchar(8000),col_collection_provider_id varchar(8000),col_created_by varchar(8000),col_last_updated_by varchar(8000),col_primary_insurance_estimate varchar(8000),col_secondary_insurance_estimate varchar(8000),col_surface_quadrant_type varchar(8000),col_appointment_sr_no varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_transaction_sr_no],[col_patient_id],[col_patient],[col_guarantor_id],[col_guarantor],[col_transaction_type],[col_transaction_date],[col_transaction_entry_date],[col_procedure_code],[col_procedure_description],[col_provider_id],[col_provider],[col_amount],[col_tooth_from],[col_tooth_to],[col_estimated_insurance_payment],[col_practice_id],[col_practice],[col_invoice_number],[col_invoice_status],[col_invoice_item],[col_quantity],[col_claim_sr_no],[col_claim],[col_pre_authorization_id],[col_payment_type],[col_surface],[col_note],[col_is_group_transaction],[col_cheque_no],[col_clinical_condition_id],[col_collection_provider_id],[col_created_by],[col_last_updated_by],[col_primary_insurance_estimate],[col_secondary_insurance_estimate],[col_surface_quadrant_type],[col_appointment_sr_no],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-18T02:50:23.207Z',
                            '2020-05-18T02:50:23.207Z',
                            'V15543',
                            '' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_transaction_sr_no varchar(8000) '$.transaction_sr_no',col_patient_id varchar(8000) '$.patient_id',col_patient varchar(8000) '$.patient',col_guarantor_id varchar(8000) '$.guarantor_id',col_guarantor varchar(8000) '$.guarantor',col_transaction_type varchar(8000) '$.transaction_type',col_transaction_date varchar(8000) '$.transaction_date',col_transaction_entry_date varchar(8000) '$.transaction_entry_date',col_procedure_code varchar(8000) '$.procedure_code',col_procedure_description varchar(8000) '$.procedure_description',col_provider_id varchar(8000) '$.provider_id',col_provider varchar(8000) '$.provider',col_amount varchar(8000) '$.amount',col_tooth_from varchar(8000) '$.tooth_from',col_tooth_to varchar(8000) '$.tooth_to',col_estimated_insurance_payment varchar(8000) '$.estimated_insurance_payment',col_practice_id varchar(8000) '$.practice_id',col_practice varchar(8000) '$.practice',col_invoice_number varchar(8000) '$.invoice_number',col_invoice_status varchar(8000) '$.invoice_status',col_invoice_item varchar(8000) '$.invoice_item',col_quantity varchar(8000) '$.quantity',col_claim_sr_no varchar(8000) '$.claim_sr_no',col_claim varchar(8000) '$.claim',col_pre_authorization_id varchar(8000) '$.pre_authorization_id',col_payment_type varchar(8000) '$.payment_type',col_surface varchar(8000) '$.surface',col_note varchar(8000) '$.note',col_is_group_transaction varchar(8000) '$.is_group_transaction',col_cheque_no varchar(8000) '$.cheque_no',col_clinical_condition_id varchar(8000) '$.clinical_condition_id',col_collection_provider_id varchar(8000) '$.collection_provider_id',col_created_by varchar(8000) '$.created_by',col_last_updated_by varchar(8000) '$.last_updated_by',col_primary_insurance_estimate varchar(8000) '$.primary_insurance_estimate',col_secondary_insurance_estimate varchar(8000) '$.secondary_insurance_estimate',col_surface_quadrant_type varchar(8000) '$.surface_quadrant_type',col_appointment_sr_no varchar(8000) '$.appointment_sr_no'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE transactions original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_transaction_sr_no] = modified.[col_transaction_sr_no],original.[col_patient_id] = modified.[col_patient_id],original.[col_patient] = modified.[col_patient],original.[col_guarantor_id] = modified.[col_guarantor_id],original.[col_guarantor] = modified.[col_guarantor],original.[col_transaction_type] = modified.[col_transaction_type],original.[col_transaction_date] = modified.[col_transaction_date],original.[col_transaction_entry_date] = modified.[col_transaction_entry_date],original.[col_procedure_code] = modified.[col_procedure_code],original.[col_procedure_description] = modified.[col_procedure_description],original.[col_provider_id] = modified.[col_provider_id],original.[col_provider] = modified.[col_provider],original.[col_amount] = modified.[col_amount],original.[col_tooth_from] = modified.[col_tooth_from],original.[col_tooth_to] = modified.[col_tooth_to],original.[col_estimated_insurance_payment] = modified.[col_estimated_insurance_payment],original.[col_practice_id] = modified.[col_practice_id],original.[col_practice] = modified.[col_practice],original.[col_invoice_number] = modified.[col_invoice_number],original.[col_invoice_status] = modified.[col_invoice_status],original.[col_invoice_item] = modified.[col_invoice_item],original.[col_quantity] = modified.[col_quantity],original.[col_claim_sr_no] = modified.[col_claim_sr_no],original.[col_claim] = modified.[col_claim],original.[col_pre_authorization_id] = modified.[col_pre_authorization_id],original.[col_payment_type] = modified.[col_payment_type],original.[col_surface] = modified.[col_surface],original.[col_note] = modified.[col_note],original.[col_is_group_transaction] = modified.[col_is_group_transaction],original.[col_cheque_no] = modified.[col_cheque_no],original.[col_clinical_condition_id] = modified.[col_clinical_condition_id],original.[col_collection_provider_id] = modified.[col_collection_provider_id],original.[col_created_by] = modified.[col_created_by],original.[col_last_updated_by] = modified.[col_last_updated_by],original.[col_primary_insurance_estimate] = modified.[col_primary_insurance_estimate],original.[col_secondary_insurance_estimate] = modified.[col_secondary_insurance_estimate],original.[col_surface_quadrant_type] = modified.[col_surface_quadrant_type],original.[col_appointment_sr_no] = modified.[col_appointment_sr_no],
                        original.updated_at = '2020-05-18T02:50:23.207Z',
                        original.office_id = 'V15543',
                        practice_name = ''
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_transaction_sr_no],[col_patient_id],[col_patient],[col_guarantor_id],[col_guarantor],[col_transaction_type],[col_transaction_date],[col_transaction_entry_date],[col_procedure_code],[col_procedure_description],[col_provider_id],[col_provider],[col_amount],[col_tooth_from],[col_tooth_to],[col_estimated_insurance_payment],[col_practice_id],[col_practice],[col_invoice_number],[col_invoice_status],[col_invoice_item],[col_quantity],[col_claim_sr_no],[col_claim],[col_pre_authorization_id],[col_payment_type],[col_surface],[col_note],[col_is_group_transaction],[col_cheque_no],[col_clinical_condition_id],[col_collection_provider_id],[col_created_by],[col_last_updated_by],[col_primary_insurance_estimate],[col_secondary_insurance_estimate],[col_surface_quadrant_type],[col_appointment_sr_no],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_transaction_sr_no],modified.[col_patient_id],modified.[col_patient],modified.[col_guarantor_id],modified.[col_guarantor],modified.[col_transaction_type],modified.[col_transaction_date],modified.[col_transaction_entry_date],modified.[col_procedure_code],modified.[col_procedure_description],modified.[col_provider_id],modified.[col_provider],modified.[col_amount],modified.[col_tooth_from],modified.[col_tooth_to],modified.[col_estimated_insurance_payment],modified.[col_practice_id],modified.[col_practice],modified.[col_invoice_number],modified.[col_invoice_status],modified.[col_invoice_item],modified.[col_quantity],modified.[col_claim_sr_no],modified.[col_claim],modified.[col_pre_authorization_id],modified.[col_payment_type],modified.[col_surface],modified.[col_note],modified.[col_is_group_transaction],modified.[col_cheque_no],modified.[col_clinical_condition_id],modified.[col_collection_provider_id],modified.[col_created_by],modified.[col_last_updated_by],modified.[col_primary_insurance_estimate],modified.[col_secondary_insurance_estimate],modified.[col_surface_quadrant_type],modified.[col_appointment_sr_no],
                        '2020-05-18T02:50:23.207Z',
                        '2020-05-18T02:50:23.207Z',
                        'V15543',
                        ''
                        );
                    