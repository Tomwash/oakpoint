
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                                CAST(BulkColumn AS NVARCHAR(MAX)) AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/D34723/patients.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_patient_id varchar(8000),col_guarantor_id varchar(8000),col_firstname varchar(8000),col_middlename varchar(8000),col_lastname varchar(8000),col_preferred_name varchar(8000),col_salutation varchar(8000),col_birthdate varchar(8000),col_status varchar(8000),col_patient_note varchar(8000),col_medical_note varchar(8000),col_alert_note varchar(8000),col_other_note varchar(8000),col_gender varchar(8000),col_address_line1 varchar(8000),col_address_line2 varchar(8000),col_city varchar(8000),col_state varchar(8000),col_zipcode varchar(8000),col_homephone varchar(8000),col_workphone varchar(8000),col_cell varchar(8000),col_email varchar(8000),col_employer_id varchar(8000),col_employer varchar(8000),col_billing_type varchar(8000),col_first_visit varchar(8000),col_last_visit varchar(8000),col_provider_id varchar(8000),col_provider varchar(8000),col_practice_id varchar(8000),col_practice varchar(8000),col_appointments varchar(8000),col_guarantor varchar(8000),col_primary_insurance_company_id varchar(8000),col_primary_insurance_company varchar(8000),col_secondary_insurance_company_id varchar(8000),col_secondary_insurance_company varchar(8000),col_primary_relationship varchar(8000),col_country varchar(8000),col_primary_medical_insurance varchar(8000),col_primary_medical_insurance_id varchar(8000),col_primary_medical_relationship varchar(8000),col_primary_medical_subscriber_id varchar(8000),col_secondary_medical_insurance varchar(8000),col_secondary_medical_insurance_id varchar(8000),col_secondary_medical_relationship varchar(8000),col_secondary_medical_subscriber_id varchar(8000),col_secondary_relationship varchar(8000),col_subscriber_id varchar(8000),col_patient_account_id varchar(8000),col_other_referral varchar(8000),col_patient_referral varchar(8000),col_referred_out varchar(8000),col_referred_by_provider_id varchar(8000),col_referred_by_patient_id varchar(8000),col_image varchar(8000),col_thumb_image varchar(8000),col_fee_no varchar(8000),col_guarantor_first_name varchar(8000),col_guarantor_last_name varchar(8000),col_created_date varchar(8000),col_preferred_contact varchar(8000),col_hygienist varchar(8000),col_integration_ids varchar(8000),col_patient_codes varchar(8000),col_primary_insurance varchar(8000),col_secondary_insurance varchar(8000),col_other_phone varchar(8000),col_preferred_communication_method varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_patient_id],[col_guarantor_id],[col_firstname],[col_middlename],[col_lastname],[col_preferred_name],[col_salutation],[col_birthdate],[col_status],[col_patient_note],[col_medical_note],[col_alert_note],[col_other_note],[col_gender],[col_address_line1],[col_address_line2],[col_city],[col_state],[col_zipcode],[col_homephone],[col_workphone],[col_cell],[col_email],[col_employer_id],[col_employer],[col_billing_type],[col_first_visit],[col_last_visit],[col_provider_id],[col_provider],[col_practice_id],[col_practice],[col_appointments],[col_guarantor],[col_primary_insurance_company_id],[col_primary_insurance_company],[col_secondary_insurance_company_id],[col_secondary_insurance_company],[col_primary_relationship],[col_country],[col_primary_medical_insurance],[col_primary_medical_insurance_id],[col_primary_medical_relationship],[col_primary_medical_subscriber_id],[col_secondary_medical_insurance],[col_secondary_medical_insurance_id],[col_secondary_medical_relationship],[col_secondary_medical_subscriber_id],[col_secondary_relationship],[col_subscriber_id],[col_patient_account_id],[col_other_referral],[col_patient_referral],[col_referred_out],[col_referred_by_provider_id],[col_referred_by_patient_id],[col_image],[col_thumb_image],[col_fee_no],[col_guarantor_first_name],[col_guarantor_last_name],[col_created_date],[col_preferred_contact],[col_hygienist],[col_integration_ids],[col_patient_codes],[col_primary_insurance],[col_secondary_insurance],[col_other_phone],[col_preferred_communication_method],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-12T01:07:33.383Z',
                            '2020-05-12T01:07:33.383Z',
                            'D34723',
                            'Carolina Smiles' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_patient_id varchar(8000) '$.patient_id',col_guarantor_id varchar(8000) '$.guarantor_id',col_firstname varchar(8000) '$.firstname',col_middlename varchar(8000) '$.middlename',col_lastname varchar(8000) '$.lastname',col_preferred_name varchar(8000) '$.preferred_name',col_salutation varchar(8000) '$.salutation',col_birthdate varchar(8000) '$.birthdate',col_status varchar(8000) '$.status',col_patient_note varchar(8000) '$.patient_note',col_medical_note varchar(8000) '$.medical_note',col_alert_note varchar(8000) '$.alert_note',col_other_note varchar(8000) '$.other_note',col_gender varchar(8000) '$.gender',col_address_line1 varchar(8000) '$.address_line1',col_address_line2 varchar(8000) '$.address_line2',col_city varchar(8000) '$.city',col_state varchar(8000) '$.state',col_zipcode varchar(8000) '$.zipcode',col_homephone varchar(8000) '$.homephone',col_workphone varchar(8000) '$.workphone',col_cell varchar(8000) '$.cell',col_email varchar(8000) '$.email',col_employer_id varchar(8000) '$.employer_id',col_employer varchar(8000) '$.employer',col_billing_type varchar(8000) '$.billing_type',col_first_visit varchar(8000) '$.first_visit',col_last_visit varchar(8000) '$.last_visit',col_provider_id varchar(8000) '$.provider_id',col_provider varchar(8000) '$.provider',col_practice_id varchar(8000) '$.practice_id',col_practice varchar(8000) '$.practice',col_appointments varchar(8000) '$.appointments',col_guarantor varchar(8000) '$.guarantor',col_primary_insurance_company_id varchar(8000) '$.primary_insurance_company_id',col_primary_insurance_company varchar(8000) '$.primary_insurance_company',col_secondary_insurance_company_id varchar(8000) '$.secondary_insurance_company_id',col_secondary_insurance_company varchar(8000) '$.secondary_insurance_company',col_primary_relationship varchar(8000) '$.primary_relationship',col_country varchar(8000) '$.country',col_primary_medical_insurance varchar(8000) '$.primary_medical_insurance',col_primary_medical_insurance_id varchar(8000) '$.primary_medical_insurance_id',col_primary_medical_relationship varchar(8000) '$.primary_medical_relationship',col_primary_medical_subscriber_id varchar(8000) '$.primary_medical_subscriber_id',col_secondary_medical_insurance varchar(8000) '$.secondary_medical_insurance',col_secondary_medical_insurance_id varchar(8000) '$.secondary_medical_insurance_id',col_secondary_medical_relationship varchar(8000) '$.secondary_medical_relationship',col_secondary_medical_subscriber_id varchar(8000) '$.secondary_medical_subscriber_id',col_secondary_relationship varchar(8000) '$.secondary_relationship',col_subscriber_id varchar(8000) '$.subscriber_id',col_patient_account_id varchar(8000) '$.patient_account_id',col_other_referral varchar(8000) '$.other_referral',col_patient_referral varchar(8000) '$.patient_referral',col_referred_out varchar(8000) '$.referred_out',col_referred_by_provider_id varchar(8000) '$.referred_by_provider_id',col_referred_by_patient_id varchar(8000) '$.referred_by_patient_id',col_image varchar(8000) '$.image',col_thumb_image varchar(8000) '$.thumb_image',col_fee_no varchar(8000) '$.fee_no',col_guarantor_first_name varchar(8000) '$.guarantor_first_name',col_guarantor_last_name varchar(8000) '$.guarantor_last_name',col_created_date varchar(8000) '$.created_date',col_preferred_contact varchar(8000) '$.preferred_contact',col_hygienist varchar(8000) '$.hygienist',col_integration_ids varchar(8000) '$.integration_ids',col_patient_codes varchar(8000) '$.patient_codes',col_primary_insurance varchar(8000) '$.primary_insurance',col_secondary_insurance varchar(8000) '$.secondary_insurance',col_other_phone varchar(8000) '$.other_phone',col_preferred_communication_method varchar(8000) '$.preferred_communication_method'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE patients original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_patient_id] = modified.[col_patient_id],original.[col_guarantor_id] = modified.[col_guarantor_id],original.[col_firstname] = modified.[col_firstname],original.[col_middlename] = modified.[col_middlename],original.[col_lastname] = modified.[col_lastname],original.[col_preferred_name] = modified.[col_preferred_name],original.[col_salutation] = modified.[col_salutation],original.[col_birthdate] = modified.[col_birthdate],original.[col_status] = modified.[col_status],original.[col_patient_note] = modified.[col_patient_note],original.[col_medical_note] = modified.[col_medical_note],original.[col_alert_note] = modified.[col_alert_note],original.[col_other_note] = modified.[col_other_note],original.[col_gender] = modified.[col_gender],original.[col_address_line1] = modified.[col_address_line1],original.[col_address_line2] = modified.[col_address_line2],original.[col_city] = modified.[col_city],original.[col_state] = modified.[col_state],original.[col_zipcode] = modified.[col_zipcode],original.[col_homephone] = modified.[col_homephone],original.[col_workphone] = modified.[col_workphone],original.[col_cell] = modified.[col_cell],original.[col_email] = modified.[col_email],original.[col_employer_id] = modified.[col_employer_id],original.[col_employer] = modified.[col_employer],original.[col_billing_type] = modified.[col_billing_type],original.[col_first_visit] = modified.[col_first_visit],original.[col_last_visit] = modified.[col_last_visit],original.[col_provider_id] = modified.[col_provider_id],original.[col_provider] = modified.[col_provider],original.[col_practice_id] = modified.[col_practice_id],original.[col_practice] = modified.[col_practice],original.[col_appointments] = modified.[col_appointments],original.[col_guarantor] = modified.[col_guarantor],original.[col_primary_insurance_company_id] = modified.[col_primary_insurance_company_id],original.[col_primary_insurance_company] = modified.[col_primary_insurance_company],original.[col_secondary_insurance_company_id] = modified.[col_secondary_insurance_company_id],original.[col_secondary_insurance_company] = modified.[col_secondary_insurance_company],original.[col_primary_relationship] = modified.[col_primary_relationship],original.[col_country] = modified.[col_country],original.[col_primary_medical_insurance] = modified.[col_primary_medical_insurance],original.[col_primary_medical_insurance_id] = modified.[col_primary_medical_insurance_id],original.[col_primary_medical_relationship] = modified.[col_primary_medical_relationship],original.[col_primary_medical_subscriber_id] = modified.[col_primary_medical_subscriber_id],original.[col_secondary_medical_insurance] = modified.[col_secondary_medical_insurance],original.[col_secondary_medical_insurance_id] = modified.[col_secondary_medical_insurance_id],original.[col_secondary_medical_relationship] = modified.[col_secondary_medical_relationship],original.[col_secondary_medical_subscriber_id] = modified.[col_secondary_medical_subscriber_id],original.[col_secondary_relationship] = modified.[col_secondary_relationship],original.[col_subscriber_id] = modified.[col_subscriber_id],original.[col_patient_account_id] = modified.[col_patient_account_id],original.[col_other_referral] = modified.[col_other_referral],original.[col_patient_referral] = modified.[col_patient_referral],original.[col_referred_out] = modified.[col_referred_out],original.[col_referred_by_provider_id] = modified.[col_referred_by_provider_id],original.[col_referred_by_patient_id] = modified.[col_referred_by_patient_id],original.[col_image] = modified.[col_image],original.[col_thumb_image] = modified.[col_thumb_image],original.[col_fee_no] = modified.[col_fee_no],original.[col_guarantor_first_name] = modified.[col_guarantor_first_name],original.[col_guarantor_last_name] = modified.[col_guarantor_last_name],original.[col_created_date] = modified.[col_created_date],original.[col_preferred_contact] = modified.[col_preferred_contact],original.[col_hygienist] = modified.[col_hygienist],original.[col_integration_ids] = modified.[col_integration_ids],original.[col_patient_codes] = modified.[col_patient_codes],original.[col_primary_insurance] = modified.[col_primary_insurance],original.[col_secondary_insurance] = modified.[col_secondary_insurance],original.[col_other_phone] = modified.[col_other_phone],original.[col_preferred_communication_method] = modified.[col_preferred_communication_method],
                        original.updated_at = '2020-05-12T01:07:33.383Z',
                        original.office_id = 'D34723',
                        practice_name = 'Carolina Smiles'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_patient_id],[col_guarantor_id],[col_firstname],[col_middlename],[col_lastname],[col_preferred_name],[col_salutation],[col_birthdate],[col_status],[col_patient_note],[col_medical_note],[col_alert_note],[col_other_note],[col_gender],[col_address_line1],[col_address_line2],[col_city],[col_state],[col_zipcode],[col_homephone],[col_workphone],[col_cell],[col_email],[col_employer_id],[col_employer],[col_billing_type],[col_first_visit],[col_last_visit],[col_provider_id],[col_provider],[col_practice_id],[col_practice],[col_appointments],[col_guarantor],[col_primary_insurance_company_id],[col_primary_insurance_company],[col_secondary_insurance_company_id],[col_secondary_insurance_company],[col_primary_relationship],[col_country],[col_primary_medical_insurance],[col_primary_medical_insurance_id],[col_primary_medical_relationship],[col_primary_medical_subscriber_id],[col_secondary_medical_insurance],[col_secondary_medical_insurance_id],[col_secondary_medical_relationship],[col_secondary_medical_subscriber_id],[col_secondary_relationship],[col_subscriber_id],[col_patient_account_id],[col_other_referral],[col_patient_referral],[col_referred_out],[col_referred_by_provider_id],[col_referred_by_patient_id],[col_image],[col_thumb_image],[col_fee_no],[col_guarantor_first_name],[col_guarantor_last_name],[col_created_date],[col_preferred_contact],[col_hygienist],[col_integration_ids],[col_patient_codes],[col_primary_insurance],[col_secondary_insurance],[col_other_phone],[col_preferred_communication_method],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_patient_id],modified.[col_guarantor_id],modified.[col_firstname],modified.[col_middlename],modified.[col_lastname],modified.[col_preferred_name],modified.[col_salutation],modified.[col_birthdate],modified.[col_status],modified.[col_patient_note],modified.[col_medical_note],modified.[col_alert_note],modified.[col_other_note],modified.[col_gender],modified.[col_address_line1],modified.[col_address_line2],modified.[col_city],modified.[col_state],modified.[col_zipcode],modified.[col_homephone],modified.[col_workphone],modified.[col_cell],modified.[col_email],modified.[col_employer_id],modified.[col_employer],modified.[col_billing_type],modified.[col_first_visit],modified.[col_last_visit],modified.[col_provider_id],modified.[col_provider],modified.[col_practice_id],modified.[col_practice],modified.[col_appointments],modified.[col_guarantor],modified.[col_primary_insurance_company_id],modified.[col_primary_insurance_company],modified.[col_secondary_insurance_company_id],modified.[col_secondary_insurance_company],modified.[col_primary_relationship],modified.[col_country],modified.[col_primary_medical_insurance],modified.[col_primary_medical_insurance_id],modified.[col_primary_medical_relationship],modified.[col_primary_medical_subscriber_id],modified.[col_secondary_medical_insurance],modified.[col_secondary_medical_insurance_id],modified.[col_secondary_medical_relationship],modified.[col_secondary_medical_subscriber_id],modified.[col_secondary_relationship],modified.[col_subscriber_id],modified.[col_patient_account_id],modified.[col_other_referral],modified.[col_patient_referral],modified.[col_referred_out],modified.[col_referred_by_provider_id],modified.[col_referred_by_patient_id],modified.[col_image],modified.[col_thumb_image],modified.[col_fee_no],modified.[col_guarantor_first_name],modified.[col_guarantor_last_name],modified.[col_created_date],modified.[col_preferred_contact],modified.[col_hygienist],modified.[col_integration_ids],modified.[col_patient_codes],modified.[col_primary_insurance],modified.[col_secondary_insurance],modified.[col_other_phone],modified.[col_preferred_communication_method],
                        '2020-05-12T01:07:33.383Z',
                        '2020-05-12T01:07:33.383Z',
                        'D34723',
                        'Carolina Smiles'
                        );
                    