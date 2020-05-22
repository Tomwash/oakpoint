
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                               '[' + CAST(BulkColumn AS NVARCHAR(MAX)) + ']' AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/streams/O22046/insurance_companies.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_insurance_company_id varchar(8000),col_insurance_company_name varchar(8000),col_group_plan_name varchar(8000),col_maximum_yearly_benefit varchar(8000),col_address_line1 varchar(8000),col_address_line2 varchar(8000),col_city varchar(8000),col_state varchar(8000),col_zipcode varchar(8000),col_phone varchar(8000),col_payer_id varchar(8000),col_practice_id varchar(8000),col_practice varchar(8000),col_group_plan_number varchar(8000),col_renew_month varchar(8000),col_employer_id varchar(8000),col_employer varchar(8000),col_country varchar(8000),col_fee_no varchar(8000),col_maximum_family_benefit varchar(8000),col_insurance_type varchar(8000),col_plan_type varchar(8000),col_sub_type varchar(8000),col_is_maximum_coverage varchar(8000),col_maximum_coverage_amount varchar(8000),col_family_deductible_limit varchar(8000),col_individual_deductible_limit varchar(8000),col_individual_ortho_maximum_limit varchar(8000),col_charge_per_visit varchar(8000),col_notes varchar(8000),col_insurance_plan_status varchar(8000),col_mail_to_insurance_company_id varchar(8000),col_pre_authorization_mail_to_insurance_company_id varchar(8000),col_eligibility varchar(8000),col_fax varchar(8000),col_default_employer varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_insurance_company_id],[col_insurance_company_name],[col_group_plan_name],[col_maximum_yearly_benefit],[col_address_line1],[col_address_line2],[col_city],[col_state],[col_zipcode],[col_phone],[col_payer_id],[col_practice_id],[col_practice],[col_group_plan_number],[col_renew_month],[col_employer_id],[col_employer],[col_country],[col_fee_no],[col_maximum_family_benefit],[col_insurance_type],[col_plan_type],[col_sub_type],[col_is_maximum_coverage],[col_maximum_coverage_amount],[col_family_deductible_limit],[col_individual_deductible_limit],[col_individual_ortho_maximum_limit],[col_charge_per_visit],[col_notes],[col_insurance_plan_status],[col_mail_to_insurance_company_id],[col_pre_authorization_mail_to_insurance_company_id],[col_eligibility],[col_fax],[col_default_employer],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-21T02:20:57.528Z',
                            '2020-05-21T02:20:57.528Z',
                            'O22046',
                            'Carolina Braces' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_insurance_company_id varchar(8000) '$.insurance_company_id',col_insurance_company_name varchar(8000) '$.insurance_company_name',col_group_plan_name varchar(8000) '$.group_plan_name',col_maximum_yearly_benefit varchar(8000) '$.maximum_yearly_benefit',col_address_line1 varchar(8000) '$.address_line1',col_address_line2 varchar(8000) '$.address_line2',col_city varchar(8000) '$.city',col_state varchar(8000) '$.state',col_zipcode varchar(8000) '$.zipcode',col_phone varchar(8000) '$.phone',col_payer_id varchar(8000) '$.payer_id',col_practice_id varchar(8000) '$.practice_id',col_practice varchar(8000) '$.practice',col_group_plan_number varchar(8000) '$.group_plan_number',col_renew_month varchar(8000) '$.renew_month',col_employer_id varchar(8000) '$.employer_id',col_employer varchar(8000) '$.employer',col_country varchar(8000) '$.country',col_fee_no varchar(8000) '$.fee_no',col_maximum_family_benefit varchar(8000) '$.maximum_family_benefit',col_insurance_type varchar(8000) '$.insurance_type',col_plan_type varchar(8000) '$.plan_type',col_sub_type varchar(8000) '$.sub_type',col_is_maximum_coverage varchar(8000) '$.is_maximum_coverage',col_maximum_coverage_amount varchar(8000) '$.maximum_coverage_amount',col_family_deductible_limit varchar(8000) '$.family_deductible_limit',col_individual_deductible_limit varchar(8000) '$.individual_deductible_limit',col_individual_ortho_maximum_limit varchar(8000) '$.individual_ortho_maximum_limit',col_charge_per_visit varchar(8000) '$.charge_per_visit',col_notes varchar(8000) '$.notes',col_insurance_plan_status varchar(8000) '$.insurance_plan_status',col_mail_to_insurance_company_id varchar(8000) '$.mail_to_insurance_company_id',col_pre_authorization_mail_to_insurance_company_id varchar(8000) '$.pre_authorization_mail_to_insurance_company_id',col_eligibility varchar(8000) '$.eligibility',col_fax varchar(8000) '$.fax',col_default_employer varchar(8000) '$.default_employer'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE insurance_companies original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_insurance_company_id] = modified.[col_insurance_company_id],original.[col_insurance_company_name] = modified.[col_insurance_company_name],original.[col_group_plan_name] = modified.[col_group_plan_name],original.[col_maximum_yearly_benefit] = modified.[col_maximum_yearly_benefit],original.[col_address_line1] = modified.[col_address_line1],original.[col_address_line2] = modified.[col_address_line2],original.[col_city] = modified.[col_city],original.[col_state] = modified.[col_state],original.[col_zipcode] = modified.[col_zipcode],original.[col_phone] = modified.[col_phone],original.[col_payer_id] = modified.[col_payer_id],original.[col_practice_id] = modified.[col_practice_id],original.[col_practice] = modified.[col_practice],original.[col_group_plan_number] = modified.[col_group_plan_number],original.[col_renew_month] = modified.[col_renew_month],original.[col_employer_id] = modified.[col_employer_id],original.[col_employer] = modified.[col_employer],original.[col_country] = modified.[col_country],original.[col_fee_no] = modified.[col_fee_no],original.[col_maximum_family_benefit] = modified.[col_maximum_family_benefit],original.[col_insurance_type] = modified.[col_insurance_type],original.[col_plan_type] = modified.[col_plan_type],original.[col_sub_type] = modified.[col_sub_type],original.[col_is_maximum_coverage] = modified.[col_is_maximum_coverage],original.[col_maximum_coverage_amount] = modified.[col_maximum_coverage_amount],original.[col_family_deductible_limit] = modified.[col_family_deductible_limit],original.[col_individual_deductible_limit] = modified.[col_individual_deductible_limit],original.[col_individual_ortho_maximum_limit] = modified.[col_individual_ortho_maximum_limit],original.[col_charge_per_visit] = modified.[col_charge_per_visit],original.[col_notes] = modified.[col_notes],original.[col_insurance_plan_status] = modified.[col_insurance_plan_status],original.[col_mail_to_insurance_company_id] = modified.[col_mail_to_insurance_company_id],original.[col_pre_authorization_mail_to_insurance_company_id] = modified.[col_pre_authorization_mail_to_insurance_company_id],original.[col_eligibility] = modified.[col_eligibility],original.[col_fax] = modified.[col_fax],original.[col_default_employer] = modified.[col_default_employer],
                        original.updated_at = '2020-05-21T02:20:57.528Z',
                        original.office_id = 'O22046',
                        practice_name = 'Carolina Braces'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_insurance_company_id],[col_insurance_company_name],[col_group_plan_name],[col_maximum_yearly_benefit],[col_address_line1],[col_address_line2],[col_city],[col_state],[col_zipcode],[col_phone],[col_payer_id],[col_practice_id],[col_practice],[col_group_plan_number],[col_renew_month],[col_employer_id],[col_employer],[col_country],[col_fee_no],[col_maximum_family_benefit],[col_insurance_type],[col_plan_type],[col_sub_type],[col_is_maximum_coverage],[col_maximum_coverage_amount],[col_family_deductible_limit],[col_individual_deductible_limit],[col_individual_ortho_maximum_limit],[col_charge_per_visit],[col_notes],[col_insurance_plan_status],[col_mail_to_insurance_company_id],[col_pre_authorization_mail_to_insurance_company_id],[col_eligibility],[col_fax],[col_default_employer],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_insurance_company_id],modified.[col_insurance_company_name],modified.[col_group_plan_name],modified.[col_maximum_yearly_benefit],modified.[col_address_line1],modified.[col_address_line2],modified.[col_city],modified.[col_state],modified.[col_zipcode],modified.[col_phone],modified.[col_payer_id],modified.[col_practice_id],modified.[col_practice],modified.[col_group_plan_number],modified.[col_renew_month],modified.[col_employer_id],modified.[col_employer],modified.[col_country],modified.[col_fee_no],modified.[col_maximum_family_benefit],modified.[col_insurance_type],modified.[col_plan_type],modified.[col_sub_type],modified.[col_is_maximum_coverage],modified.[col_maximum_coverage_amount],modified.[col_family_deductible_limit],modified.[col_individual_deductible_limit],modified.[col_individual_ortho_maximum_limit],modified.[col_charge_per_visit],modified.[col_notes],modified.[col_insurance_plan_status],modified.[col_mail_to_insurance_company_id],modified.[col_pre_authorization_mail_to_insurance_company_id],modified.[col_eligibility],modified.[col_fax],modified.[col_default_employer],
                        '2020-05-21T02:20:57.528Z',
                        '2020-05-21T02:20:57.528Z',
                        'O22046',
                        'Carolina Braces'
                        );
                    